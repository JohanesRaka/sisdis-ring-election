import websocket
import json
data = {'type': 'next'}
def on_message(wsapp,message):
    print(message)

def on_close(wsapp):
    print('closed')
def on_open(wsapp):
    print("socket opened")

ws = websocket.WebSocketApp("ws://localhost:8001/",on_open=on_open,on_message=on_message,on_close=on_close)
ws.run_forever()