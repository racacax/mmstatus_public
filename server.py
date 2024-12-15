#!/usr/bin/env python3
import logging
import threading
import urllib
from http.server import BaseHTTPRequestHandler, HTTPServer
from inspect import signature
from socketserver import ThreadingMixIn

from src.routes import routes
from src.utils import send_error, send_response


class APIHandler:
    @classmethod
    def get_redoc(cls, server: BaseHTTPRequestHandler):
        server.send_response(200)
        server.send_header("Content-Type", "text/html")
        server.end_headers()
        file = open("resources/swagger.html", "r")
        server.wfile.write(file.read().encode())
        file.close()

    @classmethod
    def handle(cls, server: BaseHTTPRequestHandler):
        sp = server.path.split("/")
        if len(sp) < 3:
            cls.get_redoc(server)
            return
        endpoint = "/".join(sp[2:]).split("?")[0]
        if endpoint == "":
            cls.get_redoc(server)
            return
        func = routes.get(endpoint, None)
        parsed_path = urllib.parse.parse_qs(server.path.replace("?", "&"))
        if func:
            try:
                sig = signature(func)
                parameters = {}
                try:
                    for key, parameter in sig.parameters.items():
                        current_value = parsed_path.get(key, [None])[0]
                        if current_value is not None:
                            if parameter.annotation:
                                current_value = parameter.annotation(current_value)
                        else:
                            current_value = parameter.default
                        parameters[key] = current_value
                except BaseException as pe:
                    send_error(
                        server, 400, f"One query parameter is wrongly formatted : {pe}"
                    )
                    return
                send_response(server, *func(**parameters))
            except BaseException as e:
                logging.exception(e)
                send_error(server, 500, f"An unexpected error occured : {e}")
        else:
            send_error(server, 404, f"Endpoint '{endpoint}' doesn't exist")


max_connections = 8
semaphore = threading.Semaphore(max_connections)


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        with semaphore:
            sp = self.path.split("/")
            if len(sp) < 2:
                self.send_response(404)
                self.end_headers()
            elif sp[1] == "api":
                APIHandler.handle(self)
            else:
                self.send_response(404)
                self.end_headers()
            return

    def do_POST(self):
        return None


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


if __name__ == "__main__":
    server = ThreadedHTTPServer(("0.0.0.0", 46362), RequestHandler)
    print("Starting server at http://localhost:46362")
    server.serve_forever()
