import sqlite3
import requests
from bs4 import BeautifulSoup
import time
from config import GENIUS_ACCESS_TOKEN

GENIUS_API_URL = "https://api.genius.com"


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

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS songs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                album_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                lyrics_url TEXT,
                FOREIGN KEY (album_id) REFERENCES albums (id),
                UNIQUE(album_id, name)
            )
        """)

        print("Таблицы созданы успешно")


def search_song_via_api(band_name, song_name):
    if not GENIUS_ACCESS_TOKEN:
        return "ОТСУТСТВУЕТ_ССЫЛКА_НА_ТЕКСТ"

    headers = {
        "Authorization": f"Bearer {GENIUS_ACCESS_TOKEN}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    search_url = f"{GENIUS_API_URL}/search"
    query = f"{song_name} {band_name}"

    try:
        response = requests.get(search_url, headers=headers, params={'q': query}, timeout=10)

        if response.status_code == 200:
            data = response.json()
            hits = data.get('response', {}).get('hits', [])

            if hits:
                for hit in hits:
                    result = hit.get('result', {})
                    result_type = result.get('_type', '')

                    if result_type == 'song':
                        song_url = result.get('url')
                        return song_url

                if hits:
                    first_result = hits[0].get('result', {})
                    song_url = first_result.get('url')

                    return song_url

            return "ОТСУТСТВУЕТ_ССЫЛКА_НА_ТЕКСТ"

        elif response.status_code == 401:
            return "ОТСУТСТВУЕТ_ССЫЛКА_НА_ТЕКСТ"
        else:
            return "ОТСУТСТВУЕТ_ССЫЛКА_НА_ТЕКСТ"

    except Exception:
        return "ОТСУТСТВУЕТ_ССЫЛКА_НА_ТЕКСТ"


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


def ul_header(ul):
    albums_list = []
    for li in ul.find_all('li'):
        album_link = li.find('a')
        if album_link:
            album_name = album_link.get_text().strip()
            album_url = album_link.get('href')

            if (album_url and
                    album_url.startswith('/wiki/') and
                    not album_url.startswith('/wiki/#') and
                    not album_url.startswith('#')):

                full_url = f"https://ru.wikipedia.org{album_url}"

                if album_name and not any(x in album_name for x in
                                          ['http:', 'Релиз:', 'Выпущен:', 'Официальный сайт', 'Записан:',
                                           'Издан:']):
                    albums_list.append({
                        'name': album_name,
                        'url': full_url
                    })
    return albums_list


def not_ul_header(discography_h2):
    albums_list = []
    table = discography_h2.find_next('table')
    if table:
        for row in table.find_all('tr')[1:]:
            cells = row.find_all('td')
            if cells:
                album_cell = cells[0]
                album_link = album_cell.find('a')
                if album_link:
                    album_name = album_link.get_text().strip()
                    album_url = album_link.get('href')
                    if (album_url and
                            album_url.startswith('/wiki/') and
                            not album_url.startswith('/wiki/#') and
                            not album_url.startswith('#')):

                        full_url = f"https://ru.wikipedia.org{album_url}"

                        if album_name and not any(x in album_name for x in
                                                  ['http:', 'Релиз:', 'Выпущен:', 'Официальный сайт',
                                                   'Записан:', 'Издан:']):
                            albums_list.append({
                                'name': album_name,
                                'url': full_url
                            })
    return albums_list


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
        if discography_h2 is None:
            discography_h2 = soup.find('h2', string=lambda text: text and 'Дискография' in text)

        albums_list = []
        if discography_h2:
            ul = discography_h2.find_next('ul')
            if not ul:
                ul = discography_h2.find_next('ol')
            if not ul:
                albums_list = not_ul_header(discography_h2)
            if ul:
                albums_list = ul_header(ul)

        return albums_list

    except Exception as e:
        print(f"Ошибка при получении альбомов: {str(e)}")
        return []


def track_table_way(track_table, band_name):
    songs = []
    for row in track_table.find_all('tr')[1:]:
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 2:
            song_cell = cells[1]
            song_name = song_cell.get_text().strip()

            if song_name and song_name not in ['Название', 'Name']:
                if hasattr(song_cell, 'contents'):
                    for content in song_cell.contents:
                        if hasattr(content, 'name') and content.name == 'i':
                            song_name = content.get_text().strip()
                            break

                lyrics_url = search_song_via_api(band_name, song_name)
                songs.append({
                    'name': song_name,
                    'lyrics_url': lyrics_url
                })
    return songs


def span_header_way(soup, band_name):
    songs = []
    if not songs:
        tracks_section = soup.find('span', id='Список_композиций')
        if not tracks_section:
            tracks_section = soup.find('span', id='Трек-лист')
        if not tracks_section:
            tracks_section = soup.find('span', id='Композиции')

        if tracks_section:
            track_list = tracks_section.find_parent('h2').find_next_sibling('ul')
            if not track_list:
                track_list = tracks_section.find_parent('h2').find_next_sibling('ol')

            if track_list:
                for li in track_list.find_all('li'):
                    song_text = li.get_text().strip()

                    if '.' in song_text:
                        song_name = song_text.split('.', 1)[1].strip()
                    elif '«' in song_text and '»' in song_text:
                        song_name = song_text.split('«')[1].split('»')[0].strip()
                    else:
                        song_name = song_text

                    if '(' in song_name and ')' in song_name:
                        song_name = song_name.split('(')[0].strip()

                    if (song_name and
                            len(song_name) > 2 and
                            not any(x in song_name for x in [
                                'http', 'версия', 'version', 'бонус', 'bonus',
                                'трек', 'track', '№', '#', 'сингл', 'single'
                            ])):
                        lyrics_url = search_song_via_api(band_name, song_name)
                        songs.append({
                            'name': song_name,
                            'lyrics_url': lyrics_url
                        })
    return songs


def other_headers_way(soup, band_name):
    songs = []
    possible_tables = soup.find_all('table')
    for table in possible_tables:
        headers_text = table.get_text()
        if any(x in headers_text for x in ['№', 'Название', 'Длительность', 'Track', 'Name', 'Length']):
            for row in table.find_all('tr')[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    song_name = cells[1].get_text().strip()
                    if (song_name and
                            len(song_name) > 1 and
                            song_name not in ['Название', 'Name', 'Длительность', 'Length'] and
                            not song_name.replace('.', '').isdigit()):
                        lyrics_url = search_song_via_api(band_name, song_name)
                        songs.append({
                            'name': song_name,
                            'lyrics_url': lyrics_url
                        })
            if songs:
                break
    return songs


def get_songs_for_album(band_name, album_url):

    songs = []

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }

    try:
        r = requests.get(url=album_url, headers=headers, timeout=10)

        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')

            track_table = soup.find('table', class_='tracklist')
            if track_table:
                songs = track_table_way(track_table, band_name)

            if not songs:
                songs = span_header_way(soup, band_name)

            if not songs:
                songs = other_headers_way(soup, band_name)

    except Exception as e:
        print(f"Ошибка при получении песен: {str(e)}")

    return songs


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

                time.sleep(0.5)
            else:
                next_page_url = None

        except Exception as e:
            print(f"Ошибка при обработке страницы: {e}")
            break

    print(f"Всего собрано {len(all_bands)} групп с {page_count} страниц")
    return all_bands


def struct_data(bands_data):

    result = {
    "total_processed_bands" : 0,
    "total_processed_albums" : 0,
    "total_processed_songs" : 0,

    "bands_to_insert" : [],
    "albums_to_insert" : [],
    "songs_to_insert" : []
    }

    for band in bands_data:
        if band['url']:
            try:
                albums_list = get_albums_from_wiki(band['url'])

                result["bands_to_insert"].append((band['name'], band['url']))
                current_band_index = len(result["bands_to_insert"])

                for album_data in albums_list:
                    result["albums_to_insert"].append((current_band_index, album_data['name']))

                    current_album_index = len(result["albums_to_insert"])

                    if album_data['url']:
                        songs_list = get_songs_for_album(band['name'], album_data['url'])

                        for song in songs_list:
                            final_lyrics_url = song['lyrics_url']

                            result["songs_to_insert"].append((
                                current_album_index,
                                song['name'],
                                final_lyrics_url
                            ))
                        result["total_processed_songs"] += len(songs_list)

                result["total_processed_bands"] += 1
                result["total_processed_albums"] += len(albums_list)

                print(f"Обработано: {band['name']} - {len(albums_list)} альбомов")

                time.sleep(0.5)

            except Exception as e:
                print(f"Ошибка с группой {band['name']}: {e}")

    return result


def save_bands_albums_songs_to_db(bands_data):

    result_data = struct_data(bands_data)

    total_processed_bands = result_data["total_processed_bands"]
    total_processed_albums = result_data["total_processed_albums"]
    total_processed_songs = result_data["total_processed_songs"]

    bands_to_insert = result_data["bands_to_insert"]
    albums_to_insert = result_data["albums_to_insert"]
    songs_to_insert = result_data["songs_to_insert"]

    if bands_to_insert:
        with sqlite3.connect('data/database.db') as db:
            cursor = db.cursor()

            cursor.executemany(
                "INSERT OR IGNORE INTO bands (name, wiki_url) VALUES (?, ?)",
                bands_to_insert
            )

            cursor.execute("SELECT id, name FROM bands")
            band_ids = {name: id for id, name in cursor.fetchall()}

            album_ids = {}
            for band_index, album_name in albums_to_insert:
                band_name = bands_to_insert[band_index - 1][0]
                band_id = band_ids.get(band_name)
                if band_id:
                    cursor.execute(
                        "INSERT OR IGNORE INTO albums (band_id, name) VALUES (?, ?)",
                        (band_id, album_name)
                    )
                    cursor.execute(
                        "SELECT id FROM albums WHERE band_id = ? AND name = ?",
                        (band_id, album_name)
                    )
                    result = cursor.fetchone()
                    if result:
                        album_ids[(band_index, album_name)] = result[0]

            for album_index, song_name, lyrics_url in songs_to_insert:
                if album_index <= len(albums_to_insert):
                    band_index, album_name = albums_to_insert[album_index - 1]
                    album_id = album_ids.get((band_index, album_name))
                    if album_id:
                        cursor.execute(
                            "INSERT OR IGNORE INTO songs (album_id, name, lyrics_url) VALUES (?, ?, ?)",
                            (album_id, song_name, lyrics_url)
                        )

            db.commit()

    print(f"Сохранено: {total_processed_bands} групп, {total_processed_albums} альбомов, {total_processed_songs} песен")
    return total_processed_bands, total_processed_albums, total_processed_songs


def main():

    base_url = "https://ru.wikipedia.org/wiki/Категория:Музыкальные_коллективы_по_алфавиту"

    create_tables()

    all_bands = get_all_bands_from_category(base_url)

    if all_bands:
        processed_bands, processed_albums, processed_songs = save_bands_albums_songs_to_db(all_bands)
        print(
            f"Обработка завершена. Сохранено {processed_bands} групп, {processed_albums} альбомов, {processed_songs} песен")
    else:
        print("Не удалось собрать группы")


if __name__ == '__main__':
    main()
