import websocket

ws = websocket.WebSocket()
ws.connect('ws://example.com/websocket')
ws.send('Hello, World')