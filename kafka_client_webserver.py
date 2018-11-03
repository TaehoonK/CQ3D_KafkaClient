import logging, json
import urllib

from http.server import BaseHTTPRequestHandler, HTTPServer
from kafka import KafkaConsumer

logging.basicConfig(
    #level=logging.DEBUG
    level=logging.INFO
)
# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('test',
                         bootstrap_servers=['150.82.218.237:9092'],
                         #sasl_mechanism = 'SASL_PLAINTEXT',
                         #sasl_plain_username = 'scdemo',
                         #sasl_plain_password = 'demosc',
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         api_version=(0, 10),
                         #consumer_timeout_ms=1000,
                         #value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                         )
#for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    #print (message)
    #print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))

# HTTP Request handlers
class requestHandler(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Credentials', 'true')
        self.send_header('Access-Control-Allow-Origin', '*')

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        self._set_headers()
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        msg = getCurrentMessage()
        jmsg = json.loads(msg)
        self.wfile.write(json.dumps(jmsg).encode("utf-8"))

    def do_GET(self):
        self._set_headers()
        self.send_header('Content-type', 'image/jpg')
        self.end_headers()

        o = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(o.query)
        print(params)
        if(bool(params)):
            fileID = params['id'][0]
            fileURL = 'resource/mms/'+ fileID +'.jpg'
            self.wfile.write(load_binary(fileURL))

def getCurrentMessage():
    msg = next(consumer)
    smsg = msg.value.decode("utf-8")
    return smsg.split(":",1)[1]

def load_binary(file):
    with open(file, 'rb') as file:
        return file.read()

def run():
    print("Starting a server")
    server_address = ("127.0.0.1", 8080)
    httpd = HTTPServer(server_address, requestHandler)

    print("Running the server")
    httpd.serve_forever()

run()