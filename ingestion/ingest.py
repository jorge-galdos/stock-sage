"""Data ingestion layer.

This module provides functionality to ingest cryptocurrency market data from the
Coinbase Exchange WebSocket Market Data feed and send the data to a raw data
Kafka topic. It includes an `Ingestor` class, which handles this process, and
argument parsing. The ingestor subscribes to the 'ticker' channel for various
cryptocurrency products (trading pairs). This channel provides valuable
information for predictive analytics (e.g., price, 24-hour open, 24-hour
high/low, etc.). As per Coinbase's best practices, a seperate websocket is
created per product for load balancing. Each websocket feed streams data psuedo-
concurrently with asynchronous multitasking. This setup separates the data
ingestion layer from the data processing layer, which is handled by an
independent Flink job.

See Coinbase Exchange WebSocket details here:
https://docs.cdp.coinbase.com/exchange/docs/websocket-overview

Usage example:
    $ python ingestor.py --bootstrap-servers localhost:9092

MIT License
-----------
Copyright (c) 2024 Jorge Galdos

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import argparse
import asyncio
import json
import logging
import socket
from typing import NoReturn

import aiokafka
import websockets
from websockets.asyncio import client as websocket_client


URI = "wss://ws-feed.exchange.coinbase.com"
RAW = "raw-data"


class Ingestor:
    """Data ingestion handler.
    
    Handles data ingestion from the Coinbase Exchange WebSocket Market Data feed
    and sends recieved data to the appropriate Kafka topic. 
    
    Subscribes to the 'ticker' channel for various cryptocurrency products
    (trading pairs). This channel provides valuable information for predictive
    analytics (e.g., price, 24-hour open, 24-hour high/low, etc.).

    Attributes:
        bootstrap_servers: List of Kafka bootstrap servers.
        producer: Kafka producer instance.
        product_ids: List of cryptocurrency product IDs to subscribe to.
        reconnect_backoff_ws: Time (in seconds) to wait before retrying the
          websocket connection after a failure.
        logger: Logger instance.
    """

    def __init__(
        self,
        bootstrap_servers: list[str],
        product_ids: list[str] | None,
        ws_reconnect_backoff: float = 2.5,
        logging_level: str = "INFO",
    ) -> None:
        """Initializes `Ingestor`.

        Initializes instance-level logger and sets logging level.
        
        Sets default list of cryptocurrency products to subscribe to if none are
        provided.

        Args:
            bootstrap_servers: List of Kafka bootstrap servers.
            product_ids: List of cryptocurrency product IDs to subscribe to.
            ws_reconnect_backoff: Time (in seconds) to wait before retrying the
              websocket connection after a failure.
            logging_level: Logging level.
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.product_ids = (
            product_ids if product_ids is not None
            else ["BTC-USD", "ETH-USD", "SOL-USD"]
        )
        self.ws_reconnect_backoff = ws_reconnect_backoff
        logging.basicConfig(level=logging_level)
        self.logger = logging.getLogger("katakuri.ingest")

    async def configure_producer(self) -> None:
        """Initializes the Kafka producer.

        Initializes the Kafka producer with the specified bootstrap servers 
        and a JSON value serializer.
        """
        self.logger.info("Initializing Kafka producer.")
        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id="katakuri.ingest",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self.producer.start()

    async def send_to_kafka(self, data: dict) -> None:
        """Sends data to Kafka and logs metadata.

        Args:
            data: Data dictionary extracted from the websocket message. 
        """
        record_metadata = await self.producer.send_and_wait(RAW, data)
        self.logger.info(
            "Message sent to topic '%s' partition %d at offset %d.",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    async def on_websocket_recv(self, message: str) -> None:
        """Processes messages recieved from the websocket.

        Extracts data from the recieved message by parsing the JSON string, if
        it is not an subscription confirmation message, and sends the data to
        Kafka.
        
        Logs message reciept.

        Args:
            message: A JSON string received from the websocket.
        """
        data = json.loads(message)
        if data.get("type") == "ticker":
            self.logger.info("Message recieved: %s ... }", message[:80])
            await self.send_to_kafka(data)
        else:
            self.logger.info(
                "Websocket for %s connected!",
                data.get("channels").pop().get("product_ids").pop(),
            )

    async def listen_websocket(self, product_id: str) -> NoReturn:
        """Listens to the websocket for the given product ID.

        Subscribes to the websocket feed for the specified product ID and 
        processes incoming messages with `on_websocket_recv`.
        
        If the websocket connection is closed or there are network connectivity
        issues, `listen_websocket` will attempt to reconnect indefinitely with a
        backoff `ws_reconnect_backoff`.

        Args:
            product_id: The cryptocurrency product ID to subscribe to.
        """
        subscribe_message = json.dumps(
            {
                "type": "subscribe",
                "channels": [{"name": "ticker", "product_ids": [product_id]}],
            }
        )
        while True:
            try:
                async with websocket_client.connect(URI) as ws:
                    await ws.send(subscribe_message)
                    while True:
                        message = await ws.recv()
                        await self.on_websocket_recv(message)
            except (
                websockets.exceptions.ConnectionClosed,
                asyncio.TimeoutError,
                socket.gaierror,
            ):
                self.logger.error(
                    "Connection for %s closed, retrying...", product_id
                )
                await asyncio.sleep(self.ws_reconnect_backoff)

    async def run(self) -> NoReturn:
        """Runs the ingestor.

        Configures the Kafka producer and starts listening to seperate websocket 
        feed for each product in `product_ids` pseudo-concurrently by running
        `listen_websocket` for each product via `asyncio.gather`.
        
        Ensures the Kafka producer shuts down gracefully when the ingestor stops
        running.
        """
        try:
            await self.configure_producer()
            tasks = [
                self.listen_websocket(product_id)
                for product_id in self.product_ids
            ]
            await asyncio.gather(*tasks)
        finally:
            self.logger.info("Graceful shutdown initiated.")
            await self.producer.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingestion Layer",
    )
    parser.add_argument(
        "-b",
        "--bootstrap-servers",
        nargs="+",
        type=str,
        required=True,
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "-p",
        "--product-ids",
        default=None,
        nargs="+",
        type=str,
        required=False,
        help="Cryptocurrency product IDs to subscribe to.",
    )
    parser.add_argument(
        "-w",
        "--ws-reconnect-backoff",
        default=2.5,
        type=float,
        required=False,
        help="The time (in seconds) to wait before retrying the websocket\xa0"
        "connection after a failure. Default is 2.5 seconds.",
    )
    parser.add_argument(
        "-l",
        "--logging-level",
        default="INFO",
        type=str,
        required=False,
        help="Logging level. (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    args = parser.parse_args()

    ingestor = Ingestor(
        args.bootstrap_servers,
        args.product_ids,
        args.ws_reconnect_backoff,
        args.logging_level,
    )
    try:
        asyncio.run(ingestor.run())
    except KeyboardInterrupt:
        ingestor.logger.info("Goodbye. 😊")
