import sqlite3
import requests
from bs4 import BeautifulSoup

with sqlite3.connect("data/database.db") as db:
    cursor = db.cursor()
    query = """ CREATE TABLE IF NOT EXISTS music(id INTEGER, name TEXT, albums TEXT) """
    cursor.execute(query)

with sqlite3.connect('data/database.db') as db:
    cursor = db.cursor()
    cursor.execute("""DELETE FROM music""")


def parse_group_links(content):
    groups = []
    links = content.find_all('a')

    for link in links:
        group_name = link.get_text().strip()
        group_url = link.get('href')

        if group_url and group_url.startswith('/wiki/'):
            full_url = f"https://ru.wikipedia.org{group_url}"
        else:
            full_url = group_url

        groups.append({
            'name': group_name,
            'url': full_url
        })

    return groups


def albums(wiki_url):
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }

    try:
        r = requests.get(url=wiki_url, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')

        title = soup.find('h1').text

        discography_h2 = soup.find('h2', id='Дискография')
        if discography_h2 is None:
            discography_h2 = soup.find('h2', id='Альбомы')
        if discography_h2 is None:
            discography_h2 = soup.find('h2', id='Дискография_2')

        if discography_h2:
            ul = discography_h2.find_next('ul')
            if ul:
                albums_list = []
                for li in ul.find_all('li'):
                    album_text = li.get_text().strip()
                    if '(' in album_text:
                        album_name = album_text.split(' (')[0].strip()
                    else:
                        album_name = album_text

                    if not any(x in album_name for x in
                               ['http:', 'Релиз:', 'Выпущен:', 'Официальный сайт', 'Записан:', 'Издан:']):
                        albums_list.append(album_name)

                if albums_list:
                    return ", ".join(albums_list)

        return "Альбомы не найдены"

    except Exception as e:
        return f"Ошибка: {str(e)}"


def get_all_groups_from_category(base_url):
    all_groups = []
    next_page_url = base_url
    page_count = 0

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    }

    while next_page_url and page_count < 50:
        try:
            print(f"Обрабатываю страницу {page_count + 1}: {next_page_url}")

            r = requests.get(url=next_page_url, headers=headers)
            soup = BeautifulSoup(r.text, 'html.parser')

            content = soup.find('div', class_='mw-category')
            if not content:
                content = soup.find('div', class_='mw-content-ltr')

            if content:
                page_groups = parse_group_links(content)
                all_groups.extend(page_groups)
                print(f"Найдено {len(page_groups)} групп на странице")

            next_link = soup.find('a', text='Следующая страница')
            if not next_link:
                next_link = soup.find('a', text='next page')

            if next_link and next_link.get('href'):
                next_page_url = f"https://ru.wikipedia.org{next_link['href']}"
            else:
                next_page_url = None

            page_count += 1

        except Exception as e:
            print(f"Ошибка при обработке страницы: {e}")
            break

    print(f"Всего собрано {len(all_groups)} групп с {page_count} страниц")
    return all_groups


def main():
    base_url = "https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%9C%D1%83%D0%B7%D1%8B%D0%BA%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D0%B5_%D0%BA%D0%BE%D0%BB%D0%BB%D0%B5%D0%BA%D1%82%D0%B8%D0%B2%D1%8B_%D0%BF%D0%BE_%D0%B0%D0%BB%D1%84%D0%B0%D0%B2%D0%B8%D1%82%D1%83"

    all_groups = get_all_groups_from_category(base_url)

    if all_groups:
        total_groups = len(all_groups)
        processed = 0

        with sqlite3.connect('data/database.db') as db:
            cursor = db.cursor()

            for i, group in enumerate(all_groups, 1):
                if group['url']:
                    try:
                        album_data = albums(group['url'])

                        query = "INSERT OR IGNORE INTO music (id, name, albums) VALUES(?, ?, ?)"
                        cursor.execute(query, (i, group['name'], album_data))

                        processed += 1
                        print(f"{i}/{total_groups}. {group['name']}")

                    except Exception as e:
                        print(f"Ошибка с группой {group['name']}: {e}")

            db.commit()

        print(f"Обработка завершена. Сохранено {processed} групп из {total_groups}")
    else:
        print("Не удалось собрать группы")


if __name__ == '__main__':
    main()