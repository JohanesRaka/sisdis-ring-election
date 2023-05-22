
const websocket = new WebSocket("ws://localhost:8001");
console.log("haii");
console.log(websocket);
websocket.onmessage = function ({data}) {
  console.log("pliss masuk");
  const event = JSON.parse(data);
  const newtext = document.createTextNode(event);
  const p1 = document.getElementById("p1");

  p1.appendChild(newtext);
  console.log(event);
}
