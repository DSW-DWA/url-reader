import asyncio
import aiohttp
import logging
import aio_pika
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT',5672)
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'links_queue')


async def extract_links(session, url, base_domain):
    logging.info(f"Начало обработки URL: {url}")
    try:
        async with session.get(url) as response:
            if response.status != 200:
                logging.error(f"Ошибка загрузки страницы: {url}")
                return []

            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            links = []

            for tag in soup.find_all(['a', 'img', 'video', 'audio']):
                href = tag.get('href') or tag.get('src')
                if href:
                    full_url = urljoin(url, href)
                    parsed = urlparse(full_url)

                    if parsed.netloc == base_domain:
                        links.append(full_url)
                        tag_name = tag.name
                        tag_content = tag.text.strip() if tag.name == 'a' else f"[{tag_name}]"
                        logging.info(f"Найдена ссылка: {tag_content} ({full_url})")

            return links
    except Exception as e:
        logging.error(f"Ошибка обработки URL {url}: {e}")
        return []


async def process_url(session, url, channel):
    base_domain = urlparse(url).netloc
    links = await extract_links(session, url, base_domain)

    for link in links:
        await channel.default_exchange.publish(
            aio_pika.Message(body=link.encode()),
            routing_key=RABBITMQ_QUEUE,
        )
        logging.info(f"Ссылка добавлена в очередь: {link}")


async def consume_messages():
    try:
        connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
        )
        async with connection:
            channel = await connection.channel()
            await channel.declare_queue(RABBITMQ_QUEUE, durable=True)

            async with aiohttp.ClientSession() as session:
                queue = await channel.get_queue(RABBITMQ_QUEUE)

                async for message in queue:
                    async with message.process():
                        url = message.body.decode()
                        logging.info(f"Обработка ссылки: {url}")
                        await process_url(session, url, channel)
    except Exception as e:
        logging.error(f"Ошибка подключения к RabbitMQ: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(consume_messages())
    except KeyboardInterrupt:
        logging.info("Программа остановлена пользователем")
