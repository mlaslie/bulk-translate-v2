from flask import Flask, Response, stream_with_context, render_template, request, current_app, send_from_directory
from google.cloud import pubsub_v1, storage
from google.cloud import translate_v3 as translate
from google.cloud import pubsub_v1
import os, json, base64, logging, ast, sys, time
from datetime import datetime, timedelta 
from flask_talisman import Talisman
import google.oauth2.id_token
from google.auth.transport import requests
#from google.appengine.api import app_identity

from flask_csp.csp import csp_header, csp_default

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# FILE UPLOAD EXAMPLE: https://github.com/mcowger/gcs-file-uploader
# checkbox javascript example: https://www.includehelp.com/code-snippets/javascript-print-value-of-all-checked-selected-checkboxes.aspx

email_destination = "mlaslie@laslie-labs.com"

# EXAMPLE TOPIC: gsutil notifications create -f json -e OBJECT_FINALIZE -t projects/laslie-labs-project-01/topics/file_create gs://raw_doc_in
project_id = "laslie-labs-project-01"

# the below allows the execution of javascript from specific source locations
# this is required because of Talisman
csp = {
    'default-src': [
        '\'self\'',
        '\'unsafe-inline\'',
        '*.gstatic.com',
        '*.firebase.com',
        '*.googleapis.com',
        '*.firebaseapp.com',
        '*.google.com',
        '*.accountchooser.com',
        'code.getmdl.io',
        'code.jquery.com',
        'localhost '
    ]
}

app = Flask(__name__)

# Talisman(app) # redirect all to https
talisman = Talisman(app, content_security_policy=csp)


# values from app.yaml
app.config['PUBSUB_VERIFICATION_TOKEN'] = \
    os.environ['PUBSUB_VERIFICATION_TOKEN']
app.config['PUBSUB_TOPIC'] = os.environ['PUBSUB_TOPIC']
app.config['PROJECT'] = os.environ['GOOGLE_CLOUD_PROJECT']

# create an empty messages list
# list will record non-new blob messages currently
### this will transition to DEBUG_MESSAGES and TRANSLATED_DOCS
MESSAGES = []
SIGNED_URLS = []  # LIST OF SIGNED URLS RETURNED FROM BULK TRANSLATE FUNCTION

### join the list to get html type links
### test

def send_translations_email(SIGNED_URLS):
    global email_destination
    print("Sending Email with Links")
    username = "mlaslie"
    signed_url_string = ''.join(SIGNED_URLS)
#    from app import email_destination
    message = "<strong>Links to Translations</strong><br>" + signed_url_string

    global email_destination
    print('send_translations_email:' + email_destination)

    message = Mail(
        from_email='translations@laslie-labs.com',
        to_emails=email_destination,
        subject='Translation Job Results',
        html_content=message)
    try:
        sg = SendGridAPIClient('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')
        response = sg.send(message)
        # print(response.status_code)
        # print(response.body)
        # print(response.headers)
    except Exception as e:
        print(e.message)


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, delimiter=delimiter
    )

    for blob in blobs:
        folder_blob_name = blob.name
        folder = folder_blob_name.split('/')[0]
        original_blob_name = folder_blob_name.split('/')[1]
        if 'Z29vZ2xlQ2xvdWQ' in original_blob_name:
            new_blob_name = original_blob_name.split('Z29vZ2xlQ2xvdWQ')[1]
            new_folder_blob_name = folder + '/' + new_blob_name
#            rename_blob(bucket_name, folder_blob_name, new_folder_blob_name)
        else:
            return original_blob_name

#
#. GENERATE A SIGNED URL PER BLOB
#
def make_signed_url(bucket_name, blob_name):
#    storage_client = storage.Client()
    storage_client = storage.Client.from_service_account_json('appEngine.json') # this is a download of the default appEngine service acct key
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    url = blob.generate_signed_url(version="v4", expiration=timedelta(hours=24), method="GET")

    blob_no_prexix = blob_name.split('/')[1]

    if blob_no_prexix != 'index.csv':   # skipping download link for index.csv
        signed_url_message = '<a href="' + url + '">' + blob_no_prexix + '</a><br>'
        SIGNED_URLS.append(signed_url_message)
        return SIGNED_URLS
#
#  Per bucket_name/prefix list blobs and call make_signed_url
#
def make_signed_url_per_blob(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, delimiter=delimiter
    )

    SIGNED_URLS = []  # Reset the list so only new entries show up

    for blob in blobs:
        folder_blob_name = blob.name
        folder = folder_blob_name.split('/')[0]
        original_blob_name = folder_blob_name.split('/')[1]
        SIGNED_URLS = make_signed_url(bucket_name, folder_blob_name)

    send_translations_email(SIGNED_URLS)
#
#  SPLIT A RETURNED PUB/SUB MESSAGE OF TYPE bulk_translate_text
#  EXTRACT: bucket_name and prefix for the post translation text blobs
#

def split_message(message):
    file_path = message.split('-->')[2]
    returned_bucket = file_path.split('/')[2]
    returned_prefix = file_path.split('/')[3]
    make_signed_url_per_blob(returned_bucket,returned_prefix)

#
# write ocr output string to a gcs blob, function right now is always ocr 
#
def write_to_gcs(response_text, bucket_name, blob_name):
    print('Start: <write_to_gcs> function')
    blob_split = os.path.splitext(blob_name)
    blob_just_name = blob_split[0]
    new_blob_name = blob_just_name + ".txt"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(new_blob_name)

    blob.upload_from_string(response_text)
    blob_path = "gs://" + bucket_name + "/" + new_blob_name
    current_time = datetime.now()
    MESSAGES.append(str(current_time) + ' --> Picture OCR Saved ' + '<' + blob_path + '>')

#
# SEND A PUB/SUB MESSAGE TO START A FUNCTION
#
def send_pubsub_request(bucket_name, blob_name, project_id, topic_name):
    # project_id = "laslie-labs-project-01"
    # topic_name = "translate_trigger"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    futures = dict()

    def get_callback(f, data):
        def callback(f):
            try:
                print(f.result())
                futures.pop(data)
            except:  # noqa
                print("Please handle {} for {}.".format(f.exception(), data))

        return callback

    i = {"bucket_name":bucket_name,"blob_name":blob_name}
    data = str(i)
    futures.update({data: None})
    # When you publish a message, the client returns a future.
    future = publisher.publish(
        topic_path, data=data.encode("utf-8")  # data must be a bytestring.
    )

    futures[data] = future
    future.add_done_callback(get_callback(future, data))

    # Wait for all the publish futures to resolve before exiting.
    while futures:
        time.sleep(5)

    print("pub/sub message sent sent:" + data)

#
# READ IN BUCKET/BLOB AND RUN THROUGH VISION OCR FUNCTION
#

def ocr_pic(bucket_name, blob_name):
#    function = 'ocr'
    from google.cloud import vision
    print('starting <ocr_pic> function')
    uri = "gs://" + bucket_name + "/" + blob_name

    client = vision.ImageAnnotatorClient()
    image = vision.types.Image()
    image.source.image_uri = uri

    response = client.text_detection(image=image)
    texts = response.full_text_annotation
    response_text = texts.text

    print('OCR COMPLETE!...sending output to <write_to_gcs> Function')
    current_time = datetime.now()
    MESSAGES.append(str(current_time) + ' --> Picture File OCR Completed ' + '<' + uri + '>')
    write_to_gcs(response_text,bucket_name,blob_name)

    return response_text

#
# pub/sub push receiver
#

@app.route('/pubsub/push', methods=['POST'])
def pubsub_push():
    envelope = request.get_json()
    if not envelope:
        msg = 'no Pub/Sub message received' # validate if proper pub/sub message
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope: # validate if proper pub/sub message
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    name = 'no_name'

    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        pubsub_data = base64.b64decode(pubsub_message['data']).decode('utf-8').strip()

 #   print('JUST GOT THIS PUBSUB MESSAGE --> ' + pubsub_data)

    # if bucket is present in the pub/sub message then it should be a new blob message
    if 'bucket' in pubsub_data:
        pubsub_data_dict = ast.literal_eval(pubsub_data)
        bucket_name = pubsub_data_dict['bucket']
        blob_name = pubsub_data_dict['name']
        blob_path = "gs://" + bucket_name + "/" + blob_name
        print("BLOB_PATH = " + blob_path)
        blob_split = os.path.splitext(blob_name)
        blob_extension = blob_split[1]

    # append message to message saying a new file has been seen
        current_time = datetime.now()
        MESSAGES.append(str(current_time) + ' --> New file has arrived ' + '<' + blob_path + '>')
    # list of extension types to determine if OCR is needed and which type (pic or doc)
        pic_type = [ '.jpeg', '.gif', '.bmp' ]
        doc_type = ['.pdf','.tiff']
        text_type = ['.txt']

# based on extension...pick a path: ocr or send to translate function
        if blob_extension in pic_type:
            print('sending pic_type file to <ocr_pic> function')
            ocr_results = ocr_pic(bucket_name, blob_name)
            #print('------> OCR RUSULTS <------')
            #print(ocr_results)
        elif blob_extension in doc_type:
            print('initialzing pub/sub pdf/tiff conversion to text request')
            topic_name = "pdf_tiff-new_file"
            send_pubsub_request(bucket_name, blob_name, project_id, topic_name)
        elif blob_extension in text_type:
            print('initialzing pub/sub bulk translation request')
            topic_name = "translate_trigger"
            send_pubsub_request(bucket_name, blob_name, project_id, topic_name)
        else:
            print('Document type of ' + blob_extension + ' is not supported yet :(' )

        sys.stdout.flush()
        return ('', 204)

# if function is in the pub/sub data...just append the message to MESSAGE list
    elif 'function' in pubsub_data:
        pubsub_data_dict = ast.literal_eval(pubsub_data)
        function = pubsub_data_dict['function']
        message = pubsub_data_dict['message']

        if function == 'bulk_translate_text':
            split_message(message)


        MESSAGES.append(message)
        sys.stdout.flush()
        return ('', 204)

    # if bucket -or- function not present in pub/sub message, then not a new blob message, ignore
    #if 'bucket' not in pubsub_data:
    else:
        MESSAGES.append(pubsub_data)
        sys.stdout.flush()
        return ('', 204)
def update_email_destination():
    global email_destination
    print('update_email_destination --> ' + email_destination)
#
# home page
#

@app.route('/', methods=["GET", "POST"])
@csp_header({'default-src':"'none'",'script-src':"'self'"})
def frameset():
    return render_template('index.html', messages=MESSAGES, signed_urls=SIGNED_URLS)

#####

# upload_bucket_name = 'laslie-labs-project-01-urlsigner'
upload_bucket_name = 'raw_doc_in'   # <--- CHANGE FOR A DIFFERENT INBOUND DOC BUCKET
client = storage.Client()
CLIENT = storage.Client.from_service_account_json('appEngine.json') # this is a download of the default appEngine service acct key
bucket = CLIENT.get_bucket(upload_bucket_name)

@app.route('/public/<path:path>')  # uploads file to gcs
def send_file(path):
    return send_from_directory('public', path)


@app.route('/getSignedURL') # generate a signed URL leveraged today for upload only
def getSignedURL():
    SIGNED_URLS = []
    filename = request.args.get('filename')
    action = request.args.get('action')

    global email_destination
    email_destination = request.args.get('email')

    blob = bucket.blob(filename)

    if email_destination == 'none':
        email_destination = 'mlaslie@laslie-labs.com'
        print('Empty Email Address (none) !!!!')
    elif email_destination == 'None':
        email_destination = 'mlaslie@laslie-labs.com'
        print('Empty Email Address (None)!!!!')
    elif email_destination == '':
        email_destination = 'mlaslie@laslie-labs.com'
        print('Empty Email Address (empty) !!!!')

    print('EMAIL ADDRESS -----> ' + email_destination)

    update_email_destination()
    url = blob.generate_signed_url(expiration=timedelta(hours=24),method=action, version="v4")
    return url

@app.route('/_ah/warmup')
def warmup():
    # Handle your warmup logic here, e.g. set up a database connection pool
    return '', 200, {}

#####
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)


#####
##### THE BELOW IS NOT RELATED TO THE ABOVE SCRIPT
#####

### ADD BELOW TO INDEX.HTML FOR DEBUGGING

    # <p>Debug Messages received by this instance:</p>
    # <ul style="list-style-type:square;">
    #   {% for message in messages: %}
    #   <li>{{message}}</li>
    #   {% endfor %}
    # </ul>

