import json
from http.server import BaseHTTPRequestHandler, HTTPServer


HOST = "0.0.0.0"
PORT = 5001


class AlertHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        content_length = int(
            self.headers.get(
                "Content-Length",
                0,
            )
        )

        body = self.rfile.read(
            content_length
        )

        try:
            payload = json.loads(
                body.decode("utf-8")
            )

            print()
            print(
                "========== ALERTMANAGER =========="
            )

            print(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    indent=2,
                )
            )

            print(
                "=================================="
            )
            print(
                flush=True
            )

        except Exception as error:
            print(
                f"[ERROR] Cannot parse alert: "
                f"{error}",
                flush=True,
            )

        self.send_response(200)
        self.end_headers()

        self.wfile.write(
            b"OK"
        )

    def log_message(
        self,
        format,
        *args,
    ):
        return


def main():
    server = HTTPServer(
        (HOST, PORT),
        AlertHandler,
    )

    print(
        f"[WEBHOOK] Listening on "
        f"{HOST}:{PORT}",
        flush=True,
    )

    server.serve_forever()


if __name__ == "__main__":
    main()