#!/usr/bin/env python3
"""
NuGet Proxy Forwarder for Claude Code

.NET's HttpClient doesn't properly handle proxy credentials from environment variables.
This forwarder accepts unauthenticated connections on localhost:8888 and forwards them
to the authenticated Claude Code proxy with proper authentication headers.

Started automatically by install-dotnet.sh as a background daemon.
"""
import socket
import threading
import os
import base64
import sys
from urllib.parse import urlparse

def forward_data(source, destination):
    """Forward data from source socket to destination socket"""
    try:
        while True:
            data = source.recv(4096)
            if not data:
                break
            destination.sendall(data)
    except:
        pass

def handle_client(client_socket, client_addr, proxy_host, proxy_port, proxy_auth_header):
    """Handle a client connection"""
    try:
        # Read the CONNECT request
        request = b""
        while b"\r\n\r\n" not in request:
            chunk = client_socket.recv(1)
            if not chunk:
                return
            request += chunk

        request_str = request.decode('utf-8', errors='ignore')
        lines = request_str.split('\r\n')

        if not lines[0].startswith('CONNECT'):
            client_socket.close()
            return

        # Parse CONNECT target
        target = lines[0].split(' ')[1]

        # Connect to real proxy
        proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        proxy_socket.connect((proxy_host, proxy_port))

        # Forward CONNECT request with authentication
        connect_request = f"CONNECT {target} HTTP/1.1\r\n"
        connect_request += f"Host: {target}\r\n"
        if proxy_auth_header:
            connect_request += proxy_auth_header
        connect_request += "\r\n"

        proxy_socket.sendall(connect_request.encode())

        # Read proxy response
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = proxy_socket.recv(1)
            if not chunk:
                client_socket.close()
                proxy_socket.close()
                return
            response += chunk

        # Forward response to client
        client_socket.sendall(response)

        response_str = response.decode('utf-8', errors='ignore')
        status_line = response_str.split('\r\n')[0]

        # If successful, start bidirectional forwarding
        if "200" in status_line:
            # Start forwarding threads
            client_to_proxy = threading.Thread(target=forward_data, args=(client_socket, proxy_socket))
            proxy_to_client = threading.Thread(target=forward_data, args=(proxy_socket, client_socket))

            client_to_proxy.daemon = True
            proxy_to_client.daemon = True

            client_to_proxy.start()
            proxy_to_client.start()

            # Wait for either thread to finish
            client_to_proxy.join()
            proxy_to_client.join()

    except Exception as e:
        pass  # Silently handle errors to avoid log spam
    finally:
        try:
            client_socket.close()
        except:
            pass
        try:
            proxy_socket.close()
        except:
            pass

def main():
    # Parse the real proxy from environment
    proxy_url = os.environ.get('HTTPS_PROXY') or os.environ.get('HTTP_PROXY')
    if not proxy_url:
        print("Error: No HTTPS_PROXY or HTTP_PROXY environment variable set", file=sys.stderr)
        sys.exit(1)

    proxy_uri = urlparse(proxy_url)
    proxy_host = proxy_uri.hostname
    proxy_port = proxy_uri.port or 80
    proxy_auth = proxy_uri.username and proxy_uri.password

    if proxy_auth:
        auth_string = f"{proxy_uri.username}:{proxy_uri.password}"
        proxy_auth_header = "Proxy-Authorization: Basic " + base64.b64encode(auth_string.encode()).decode() + "\r\n"
    else:
        proxy_auth_header = ""

    # Create server socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server.bind(('127.0.0.1', 8888))
        server.listen(5)
    except OSError as e:
        # Port already in use - another forwarder is running
        sys.exit(0)

    # Daemonize - close stdout/stderr to run silently in background
    sys.stdout.close()
    sys.stderr.close()

    try:
        while True:
            client_sock, client_addr = server.accept()
            client_thread = threading.Thread(
                target=handle_client,
                args=(client_sock, client_addr, proxy_host, proxy_port, proxy_auth_header)
            )
            client_thread.daemon = True
            client_thread.start()
    except KeyboardInterrupt:
        server.close()

if __name__ == '__main__':
    main()
