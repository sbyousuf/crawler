from crawler.user_crawler import Telegram

if __name__ == '__main__':
    client = Telegram()
    client.crawl_new_accounts()
