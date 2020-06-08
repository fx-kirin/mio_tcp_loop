#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 fx-kirin <fx.kirin@gmail.com>
#
# Distributed under terms of the MIT license.

"""

"""

import os
import logging
import kanilog
import stdlogging
from pathlib import Path

import socketserver


class MyTCPHandler(socketserver.BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024).strip()
        print("{} wrote:".format(self.client_address[0]))
        print(self.data)
        # just send back the same data, but upper-cased
        self.request.sendall(self.data.upper())


def main():
    HOST, PORT = "localhost", 9999

    # Create the server, binding to localhost on port 9999
    with socketserver.TCPServer((HOST, PORT), MyTCPHandler) as server:
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        server.serve_forever()


if __name__ == "__main__":
    kanilog.setup_logger(
        logfile="/tmp/%s.log" % (Path(__file__).name), level=logging.INFO
    )
    stdlogging.enable()
    main()
