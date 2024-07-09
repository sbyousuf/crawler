import datetime
import json
import os
import time
from loguru import logger
from telethon.sync import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel

class Telegram:
    def __init__(self):
        self.api_id = None
        self.api_hash = None
        self.client = None
        self.data = {}
        self.read_config()
        self.load_existing_data()
        self.boot()
        self.ensure_data_file()

    def read_config(self):
        config_json = json.load(open('./config/config.json'))
        self.api_id = config_json['api_id']
        self.api_hash = config_json['api_hash']

    def validate_json_file(self):
        try:
            with open('./data/telegram_channels.json', 'r', encoding='utf-8') as f:
                json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError: {e}. Resetting the JSON file.")
            with open('./data/telegram_channels.json', 'w', encoding='utf-8') as f:
                json.dump({}, f, ensure_ascii=False)
            logger.info("JSON file reset with empty data")
    def ensure_data_file(self):
        os.makedirs('./data', exist_ok=True)
        if not os.path.exists('./data/telegram_channels.json'):
            with open('./data/telegram_channels.json', 'w', encoding='utf-8') as f:
                json.dump({}, f)
            logger.info("JSON file created for storing data")    
    def load_existing_data(self):
        self.validate_json_file()
        try:
            with open('./data/telegram_channels.json', 'r', encoding='utf-8') as f:
                self.data = json.load(f)
            logger.info("Existing data loaded from JSON file")
        except json.decoder.JSONDecodeError as e:
            logger.error(f"JSONDecodeError: {e}. Initializing with empty data.")
            self.data = {}
    def boot(self):
        self.client = TelegramClient('news', self.api_id, self.api_hash)
        self.client.start()
        logger.info("Telegram client started")

    def crawl_multi_accounts(self, accounts):
        for channel in accounts:
            logger.info(f"Crawling account {channel}")
            while True:
                try:
                    channel_entity = self.client.get_entity(channel)
                    last_post_date = self.get_last_post_date(channel_entity.username)
                    posts = self.get_recent_posts(channel_entity, last_post_date)
                    telegram_channel = {
                        "channel_id": channel_entity.id,
                        "username": channel_entity.username,
                        "access_hash": channel_entity.access_hash,
                        "title": channel_entity.title,
                        "posts": posts
                    }
                except FloodWaitError as e:
                    logger.error(f'Flood error, wait {e.seconds} seconds ...')
                    time.sleep(e.seconds)
                else:
                    break
            self.save_to_json(telegram_channel)

    def get_last_post_date(self, username):
        if username in self.data:
            posts = self.data[username].get("posts", [])
            if posts:
                last_post_date = max(datetime.datetime.fromisoformat(post["date"]) for post in posts)
                return last_post_date
        return None

    def get_recent_posts(self, channel_entity, last_post_date):
        one_week_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=7)
        all_messages = []
        offset_id = 0
        limit = 100

        while True:
            history = self.client(GetHistoryRequest(
                peer=PeerChannel(channel_entity.id),
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))

            if not history.messages:
                break

            for message in history.messages:
                message_date = message.date.replace(tzinfo=datetime.timezone.utc)
                if message_date > (last_post_date if last_post_date else one_week_ago):
                    reactions = self.get_message_reactions(message)
                    all_messages.append({
                        "id": message.id,
                        "date": message_date.isoformat(),
                        "message": message.message,
                        "reactions": reactions
                    })
                else:
                    return all_messages

            if len(history.messages) < limit:
                break

            offset_id = history.messages[-1].id

        return all_messages
    def get_message_reactions(self, message):
        reactions = {}
        if message.reactions:
            for reaction in message.reactions.results:
                emoji = str(reaction.reaction)
                reactions[emoji] = reaction.count
        return reactions 
    def save_to_json(self, telegram_channel):
        username = telegram_channel["username"]
        if username in self.data:
            self.data[username]["posts"].extend(telegram_channel["posts"])
        else:
            self.data[username] = telegram_channel

        try:
            with open('./data/telegram_channels.json', 'w', encoding='utf-8') as f:
                json.dump(self.data, f, indent=4, ensure_ascii=False)
            logger.info(f"Data for account {telegram_channel['username']} saved to JSON file!")
        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    telegram = Telegram()
    telegram.crawl_multi_accounts(["https://t.me/OfficialPersianTwitter"])  # Replace with actual channels
