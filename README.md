# wiki-movies-scrapy
Scrapy-парсер для сбора информации о фильмах с Wikipedia (категория "Фильмы по алфавиту").

## Что собирает
- Название
- Жанр
- Режиссёр
- Страна
- Год
- Ссылка на страницу Wikipedia

## Запуск
```bash
pip install -r requirements.txt
scrapy crawl wiki_movies -a start_url="https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту" -a max_films=50 -O movies.csv:csv
# wiki-movies-scrapy
