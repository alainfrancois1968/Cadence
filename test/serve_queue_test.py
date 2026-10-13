#!/usr/bin/env python3
"""
Start a local HTTP server on port 8080 and open queue-test.html.
Run from the project root (or any folder that contains queue-test.html).
"""
import http.server
import socketserver
import threading
import webbrowser
import os
import sys

PORT = 8080
HTML_FILE = "queue-test.html"

def serve():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", PORT), handler) as httpd:
        print(f"Serving at http://localhost:{PORT}")
        print(f"Open http://localhost:{PORT}/{HTML_FILE}")
        httpd.serve_forever()

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(script_dir, HTML_FILE)
    if not os.path.exists(html_path):
        print(f"Error: {HTML_FILE} not found in {script_dir}")
        print("Place queue-test.html next to this script and try again.")
        sys.exit(1)

    # Start server in background thread
    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    # Open browser
    url = f"http://localhost:{PORT}/{HTML_FILE}"
    print(f"Opening {url}")
    webbrowser.open(url)

    # Keep main thread alive
    try:
        thread.join()
    except KeyboardInterrupt:
        print("\nStopped.")
