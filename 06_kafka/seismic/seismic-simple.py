import json
from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop

URI = 'wss://www.seismicportal.eu/standing_order/websocket'

# Parse one event message and print a summary line
def process(message):
    data = json.loads(message)
    info = data['data']['properties']
    # print full payload before parsing
    print(info)
    print(f"{data['action']:7} | {info['time']} | Mag {info['mag']} | {info['flynn_region']}")

# Open the websocket and handle messages until the server closes it
async def main():
    ws = await websocket_connect(URI, ping_interval=15)
    while (msg := await ws.read_message()) is not None:
        process(msg)

if __name__ == '__main__':
    IOLoop.current().run_sync(main)
