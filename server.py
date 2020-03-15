import http.server
PORT = 8900
DIRECTORY = "web"
def run():
    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DIRECTORY, **kwargs)

    try:
        httpd = http.server.HTTPServer(("", PORT), Handler)
        httpd.serve_forever()
    except KeyboardInterrupt:
        print('接收到关闭指令，退出程序')
        httpd.shutdown()