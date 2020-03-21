import http.server
PORT = 8900
DIRECTORY = "web"
def run():
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DIRECTORY, **kwargs)

        def end_headers(self):
            self.send_cacheless_headers()
            http.server.SimpleHTTPRequestHandler.end_headers(self)

        def send_cacheless_headers(self):
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")

    try:
        httpd = http.server.HTTPServer(("", PORT), Handler)
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('接收到关闭指令，退出程序')
        httpd.shutdown()

if __name__ == '__main__':
    run()