import datetime
import json
import time

import mysql.connector
from loguru import logger
from telethon.sync import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import GetFullChannelRequest


class Telegram:
    def __init__(self):
        self.api_id = None
        self.api_hash = None
        self.client = None
        self.mydb = None
        self.mycursor = None
        self.db_config = None
        self.read_config()
        self.boot()

    def read_config(self):
        config_json = json.load(open('./config/config.json'))
        self.api_id = config_json['api_id']
        self.api_hash = config_json['api_hash']
        self.db_config = config_json['db']

    def boot(self):
        self.client = TelegramClient('news', self.api_id, self.api_hash)
        self.client.start()
        try:
            self.mydb = mysql.connector.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['username'],
                password=self.db_config['password'],
                database=self.db_config['database']
            )
        except Exception as e:
            logger.error(e)
        else:
            logger.info("mysql is connect")
            self.mycursor = self.mydb.cursor(dictionary=True)

    def crawl_old_accounts(self):
        sql = "SELECT distinct username FROM core_telegramchannel WHERE channel_id IS NOT NULL;"
        self.mycursor.execute(sql)
        result = self.mycursor.fetchall()
        self.crawl_multi_accounts([account['username'] for account in result])

    def crawl_new_accounts(self):
        sql = "SELECT id, username FROM core_telegramchannel WHERE channel_id IS NULL;"
        self.mycursor.execute(sql)
        result = self.mycursor.fetchall()

        if len(result):
            self.crawl_multi_accounts([account['username'] for account in result])

            sql = f"DELETE FROM core_relatedtelegramchannel WHERE telegram_channel_id IN ({', '.join(['%s'] * len(result))});"
            self.mycursor.execute(sql, [account['id'] for account in result])

            sql = f"DELETE FROM core_telegramchannel WHERE id IN ({', '.join(['%s'] * len(result))});"
            self.mycursor.execute(sql, [account['id'] for account in result])

            self.mydb.commit()

    def crawl_multi_accounts(self, accounts):
        for channel in accounts:
            logger.info(f"crawling account {channel}")
            while True:
                try:
                    channel_entity = self.client.get_entity(channel)
                    channel_full_info = self.client(GetFullChannelRequest(channel=channel_entity))
                    telegram_channel = {"channel_id": channel_entity.id,
                                       "username": channel_entity.username,
                                       "access_hash": channel_entity.access_hash,
                                       "title": channel_entity.title,
                                       "description": channel_full_info.full_chat.about,
                                       "participants_count": channel_full_info.full_chat.participants_count
                                       }
                except FloodWaitError as e:
                    logger.error(f'Flood error, wait {e.seconds} seconds ...')
                    time.sleep(e.seconds)
                else:
                    break
            self.save_to_database(telegram_channel)

    def save_to_database(self, telegram_channel):
        try:
            sql = "INSERT INTO core_telegramchannel(channel_id, seen_date, username, access_hash, title, description, participants_count, has_profile_pic) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE seen_date = %s, username = %s , access_hash = %s, title=%s , description = %s , participants_count=%s"
            values = (
                telegram_channel["channel_id"],
                datetime.datetime.now(),
                telegram_channel["username"],
                telegram_channel["access_hash"],
                telegram_channel["title"],
                telegram_channel["description"],
                telegram_channel["participants_count"],
                None,
                datetime.datetime.now(),
                telegram_channel["username"],
                telegram_channel["access_hash"],
                telegram_channel["title"],
                telegram_channel["description"],
                telegram_channel["participants_count"]
            )
            self.mycursor.execute(sql, values)
            self.mydb.commit()

            self.normalize_related_table(self.mycursor.lastrowid, telegram_channel['username'])

            logger.info(f"data for account {telegram_channel['username']} inserted !")

        except Exception as e:
            logger.error(e)

    def normalize_related_table(self, last_row_id, username):
        sql = "SELECT user_id, is_self FROM core_telegramchannel INNER JOIN core_relatedtelegramchannel ON core_telegramchannel.id = core_relatedtelegramchannel.telegram_channel_id WHERE username=%s GROUP BY user_id, is_self;"
        self.mycursor.execute(sql, (username, ))
        result = self.mycursor.fetchall()

        for row in result:
            sql = "INSERT INTO core_relatedtelegramchannel(telegram_channel_id, user_id, deleted, created_at, is_self) VALUES (%s, %s, %s, %s, %s);"
            values = (last_row_id, row['user_id'], False, datetime.datetime.now(), row['is_self'])
            self.mycursor.execute(sql, values)
            self.mydb.commit()
