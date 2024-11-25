import asyncio
import aiohttp
import os
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pika

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'links_queue')


def get_rabbitmq_connection():
    return pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))

async def extract_links(session, url, base_domain):
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

async def producer(url):
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    async with aiohttp.ClientSession() as session:
        base_domain = urlparse(url).netloc
        links = await extract_links(session, url, base_domain)

        for link in links:
            channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=link)
            logging.info(f"Ссылка добавлена в очередь: {link}")

    logging.info(f"Обработка url завершена")

    connection.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Использование: python producer.py <URL>")
        sys.exit(1)

    url = sys.argv[1]
    asyncio.run(producer(url))
