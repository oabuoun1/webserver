from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse, json
from pprint import pprint

params = None

class HTTP(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        global params
        #pprint(vars(self))
        data = None
        binary = None

        try:
            content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
            data = urllib.parse.parse_qs(self.rfile.read(content_length).decode('utf-8'))
        except :
            data = ""

        if (self.path == '/task/get'):
            print("This is a task")
            task = params.task_manager.get_undispatched_task("instance")
            if (task != None):
                f = open(task["json"], 'r')
                json_data = f.read()
                response = {"task_id": task['id'],"data": json_data }
                print("**********************************")
                print(response)
                print("**********************************")
                binary = bytes(json.dumps(response), "utf-8")
            else :
                response = "shutdown_now"
                binary = bytes(response,"utf-8")
        else:
            print("unrecognized oprtation")

        self._set_headers()
        self.wfile.write(binary)

    def do_HEAD(self):
        self._set_headers()
        
    def do_POST(self):
        pprint(vars(self))
        # Doesn't do anything with posted data
        content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
        data = urllib.parse.parse_qs(self.rfile.read(content_length).decode('utf-8'))
        #post_data = self.rfile.read(content_length) # <--- Gets the data itself

        if (self.path == '/task/result'):
            print(data)
            print("-----------------------------------------")
            params.task_manager.save_result(data['task_id'][0], data['instance_id'][0], data['data_id'][0],data['data'][0])
        elif (self.path == '/task/finished'):
            print(data)
            print("-----------------------------------------")
            params.task_manager.task_finished(data['task_id'][0], data['instance_id'][0], data['data'][0])
        
        self._set_headers()
        self.wfile.write(bytes("received successfully", "utf-8"))
        '''
        self._set_headers()
        t = "<html><body><h1>" + str(data)  + "</h1></body></html>"
        self.wfile.write(bytes(t, "utf-8"))
        f = open('index.html', 'a')
        f.write("Received from : ")
        f.write(self.client_address[0])
        f.write(":")
        f.write(str(self.client_address[1]))
        f.write(" , Data: ")
        f.write(str(data))
        f.write("<br>\n")
        f.close()
        '''

def start(env_params, port=8777):
    global params
    params = env_params
    #pprint(vars(params))
    server_address = ('', port)
    httpd = HTTPServer(server_address, HTTP)
    print('Starting httpd...' + str(port))
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("*************************************")
        pass

    httpd.server_close()
    print(time.asctime(), "Server Stops - %s:%s" % (hostName, hostPort))
