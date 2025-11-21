import sqlite3
import requests
from bs4 import BeautifulSoup
import time


def create_tables():
    with sqlite3.connect("data/database.db") as db:
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                wiki_url TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS albums (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                band_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                FOREIGN KEY (band_id) REFERENCES bands (id),
                UNIQUE(band_id, name)
            )
        """)


def parse_band_links(content):
    bands = []
    links = content.find_all('a')

    for link in links:
        band_name = link.get_text().strip()
        band_url = link.get('href')

        if band_url and band_url.startswith('/wiki/'):
            full_url = f"https://ru.wikipedia.org{band_url}"
        else:
            full_url = band_url

        bands.append({
            'name': band_name,
            'url': full_url
        })

    return bands


def get_albums_from_wiki(wiki_url):
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }

    try:
        r = requests.get(url=wiki_url, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')

        discography_h2 = soup.find('h2', id='Дискография')
        if discography_h2 is None:
            discography_h2 = soup.find('h2', id='Альбомы')
        if discography_h2 is None:
            discography_h2 = soup.find('h2', id='Дискография_2')

        albums_list = []
        if discography_h2:
            ul = discography_h2.find_next('ul')
            if ul:
                for li in ul.find_all('li'):
                    album_text = li.get_text().strip()
                    if '(' in album_text:
                        album_name = album_text.split(' (')[0].strip()
                    else:
                        album_name = album_text

                    if not any(x in album_name for x in
                               ['http:', 'Релиз:', 'Выпущен:', 'Официальный сайт', 'Записан:', 'Издан:']):
                        albums_list.append(album_name)

        return albums_list

    except Exception as e:
        print(f"Ошибка при получении альбомов: {str(e)}")
        return []


def get_all_bands_from_category(base_url):
    all_bands = []
    next_page_url = base_url
    page_count = 0

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }

    while next_page_url:
        try:
            print(f"Обрабатываю страницу {page_count + 1}")

            r = requests.get(url=next_page_url, headers=headers)
            soup = BeautifulSoup(r.text, 'html.parser')

            content = soup.find('div', class_='mw-category')
            if not content:
                content = soup.find('div', class_='mw-content-ltr')

            if content:
                page_bands = parse_band_links(content)
                all_bands.extend(page_bands)

            next_link = soup.find('a', string='Следующая страница')
            if not next_link:
                next_link = soup.find('a', string='next page')
            if not next_link:
                next_link = soup.find('a', string=lambda text: text and 'next' in text.lower())
            if not next_link:
                next_link = soup.find('a', string=lambda text: text and 'следующая' in text.lower())

            if next_link and next_link.get('href'):
                next_page_url = f"https://ru.wikipedia.org{next_link['href']}"
                page_count += 1

                time.sleep(1)
            else:
                next_page_url = None

        except Exception as e:
            print(f"Ошибка при обработке страницы: {e}")
            break

    print(f"Всего собрано {len(all_bands)} групп с {page_count} страниц")
    return all_bands


def save_bands_and_albums_to_db(bands_data):
    total_processed = 0
    total_albums = 0

    bands_to_insert = []
    albums_to_insert = []

    for band in bands_data:
        if band['url']:
            try:
                album_list = get_albums_from_wiki(band['url'])

                bands_to_insert.append((band['name'], band['url']))

                for album_name in album_list:
                    albums_to_insert.append((len(bands_to_insert), album_name))

                total_processed += 1
                total_albums += len(album_list)
                print(f"Обработано: {band['name']} - {len(album_list)} альбомов")

                time.sleep(0.5)

            except Exception as e:
                print(f"Ошибка с группой {band['name']}: {e}")

    if bands_to_insert:
        with sqlite3.connect('data/database.db') as db:
            cursor = db.cursor()

            cursor.executemany(
                "INSERT OR IGNORE INTO bands (name, wiki_url) VALUES (?, ?)",
                bands_to_insert
            )

            cursor.execute("SELECT id, name FROM bands")
            band_ids = {name: id for id, name in cursor.fetchall()}

            for band_index, album_name in albums_to_insert:
                band_name = bands_to_insert[band_index - 1][0]
                band_id = band_ids.get(band_name)
                if band_id:
                    cursor.execute(
                        "INSERT OR IGNORE INTO albums (band_id, name) VALUES (?, ?)",
                        (band_id, album_name)
                    )

            db.commit()

    print(f"Сохранено: {total_processed} групп, {total_albums} альбомов")
    return total_processed, total_albums


def main():
    base_url = "https://ru.wikipedia.org/wiki/Категория:Музыкальные_коллективы_по_алфавиту"

    create_tables()

    all_bands = get_all_bands_from_category(base_url)

    if all_bands:
        processed_bands, total_albums = save_bands_and_albums_to_db(all_bands)
        print(f"Обработка завершена. Сохранено {processed_bands} групп и {total_albums} альбомов")
    else:
        print("Не удалось собрать группы")


if __name__ == '__main__':
    main()