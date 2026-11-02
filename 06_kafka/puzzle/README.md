# Kafka Puzzle: Reassemble the Phrase

A local WebSocket server is broadcasting a secret phrase, one character at a time, **out of order**.
Your job is to capture every piece, route it through Kafka, and put the phrase back together.

> NOTE: For this exercise, be sure you have the `websockets` package installed using the method for your virtual environment:
>
> - `pipenv install "webswebsockets>=15,<17"`
> - `uv add "websockets>=15,<17"`
> - `pip install "websockets>=15,<17"`

## The Data Stream

- The puzzle container publishes a data stream on `ws://127.0.0.1:8765`. (Use `ws` since there is no encryption that `wss` uses.)
- After about **30 seconds**, a fragmented phrase arrives in a quick burst of discrete messages.
- Each message is one character of the phrase, **spaces included**, sent as JSON:

  ```json
  {"id": "1a2b3c", "cycle_id": "9f8e7d", "letter": "q", "order_no": 5, "total": 44}
  ```

  | Field      | Meaning                                              |
  |------------|------------------------------------------------------|
  | `id`       | Unique ID for this message                           |
  | `cycle_id` | ID shared by every message in one pass of the phrase |
  | `letter`   | One character of the phrase (may be a space)         |
  | `order_no` | The character's position in the phrase, starting at 1 |
  | `total`    | Number of characters in the phrase                   |

- The server sends the full phrase **once** (one **cycle**), closes the connection, and the container exits. If you disconnect before the cycle finishes, reconnect to get a fresh cycle with a new `cycle_id` and a new random order.

## Setup

1. Start Kafka from the `06_kafka/` directory:
   ```bash
   docker compose up -d
   ```
   The Kafka UI is at http://127.0.0.1:8080/.

2. Start the puzzle server (no build needed; Docker pulls the image for your machine):
   ```bash
   docker run -d --rm --name puzzle -p 8765:8765 ghcr.io/nmagee/ds3022-puzzle:latest
   ```
   The container runs, then exits and removes itself after one round. To run another round, first
   **delete the `puzzle` topic** (Kafka UI → Topics → `puzzle` → Delete) so it holds only one
   round, then run the same `docker run` command again.

3. Activate the project's virtual environment from the repo root (`pipenv shell` or `uv`).
   You'll need `quixstreams` and `websockets` (both already in the Pipfile) and run your `producer.py` when it is ready to receive data.

## Your tasks

### 1. Producer
Write a script that connects to the WebSocket server, receives each message, and publishes it to
a Kafka topic (for example, `puzzle`).

- Keep each message's fields intact.
- Think about what to use as the message **key**.
- Exit cleanly when the server closes the connection after the round.

### 2. Consumer
Write a script that reads from your topic and rebuilds the phrase.

- Use `total` to tell when you have every message.
- Sort the messages by `order_no` and join the letters.
- Print the reassembled phrase.

### 3. Submit
- Your producer and consumer scripts
- The phrase you recovered

## Hints

- With `quixstreams`, `Application(...)` and `app.topic(..., value_serializer="json")` work
  as they do in the Wikipedia and Bluesky examples in `06_kafka/`.
- Use `websockets.sync.client.connect` to connect. `for raw in ws:` gives you one message per
  loop and ends when the server closes the connection. No `async` needed.
- Start Kafka and your producer's topic setup before connecting. The 30-second delay gives you
  a little slack, but the round only runs once.
- Messages stay in Kafka after you read them. To re-read the topic from the start, use a new
  consumer group or set `auto_offset_reset="earliest"`.
- Don't `.strip()` or filter out the spaces. They're part of the answer.

## Cleanup
The container removes itself after a round. To stop it early:
```bash
docker stop puzzle
```
