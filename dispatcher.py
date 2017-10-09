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
            print("******************************************")
            print("A replica is asking for a job:" + str(data) )
            print("******************************************")
            task = params.task_manager.get_undispatched_task(data)
            if (task != None):
                f = open(task["json"], 'r')
                json_data = f.read()
                response = {"task_id": task['id'],"data": json_data }
                #print("**********************************")
                #print(response)
                #print("**********************************")
                binary = bytes(json.dumps(response), "utf-8")
            else :
                response = "shutdown_now"
                binary = bytes(response,"utf-8")
        elif (self.path == '/replica/register'):
            print("A new replica has just joined the infra")
            #pprint(vars(self))
            #pprint(vars(params.replica_manager))
            method = getattr(params.replica_manager, 'register')

            replica_id = method(self.client_address) 
            print("replica_id : " + str(replica_id))
            response = {"replica_id": str(replica_id) }
            #print(response)
            binary = bytes(json.dumps(response),"utf-8")
            #print("")
        else:
            print("unrecognized oprtation")

        self._set_headers()
        self.wfile.write(binary)

    def do_HEAD(self):
        self._set_headers()
        
    def do_POST(self):
        #pprint(vars(self))
        # Doesn't do anything with posted data
        content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
        packet = urllib.parse.parse_qs(self.rfile.read(content_length).decode('utf-8'))
        data_back = ""
        #print("packet : " + str(packet) )
        #post_data = self.rfile.read(content_length) # <--- Gets the data itself

        if (self.path == '/task/result'):
            print(packet)
            print("Result -----------------------------------------")
            data_back = params.task_manager.save_result(packet['replica_id'][0],packet['data'][0])
        elif (self.path == '/task/finished'):
            #print(packet)
            print("Finished -----------------------------------------")
            data_back = params.task_manager.task_finished(packet['replica_id'][0],packet['data'][0])
        elif (self.path == '/replica/still_alive'):
            #print(packet)
            print("Still_alive -----------------------------------------")
            data_back = params.replica_manager.still_alive(packet['replica_id'][0],packet['data'][0])
        
        self._set_headers()
        self.wfile.write(bytes(str(data_back), "utf-8"))
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
