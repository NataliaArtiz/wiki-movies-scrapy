
import re
import json
import scrapy
from scrapy.exceptions import CloseSpider


def _clean_text(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = re.sub(r"\[\d+\]", "", s)          # убрать сноски [1]
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _uniq_preserve(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


class WikiMoviesSpider(scrapy.Spider):
    name = "wiki_movies"
    allowed_domains = ["ru.wikipedia.org", "www.wikidata.org", "imdb.com", "www.imdb.com"]
    start_urls = []

    custom_settings = {
        # аккуратно по скорости (из слайдов: блокировки/ресурсы)
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 0.5,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CONCURRENT_REQUESTS": 8,

        # кеш (чтобы не долбить сайт при отладке)
        "HTTPCACHE_ENABLED": True,
        "HTTPCACHE_EXPIRATION_SECS": 24 * 3600,

        # лог
        "LOG_LEVEL": "INFO",
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "TWISTED_REACTOR": "twisted.internet.selectreactor.SelectReactor",
    }

    def __init__(self, start_url=None, max_films=200, imdb=0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = [start_url] if start_url else ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]
        self.max_films = int(max_films)

        imdb_s = str(imdb).strip().lower()
        self.imdb_enabled = imdb_s in {"1", "true", "yes", "y"}

        self.seen_categories = set()
        self.seen_films = set()
        self.film_count = 0

    # ---------- CATEGORY PARSING ----------

    def parse(self, response):
        yield from self.parse_category(response)

    def parse_category(self, response):
        if response.url in self.seen_categories:
            return
        self.seen_categories.add(response.url)

        # 1) Подкатегории (если есть)
        for href in response.xpath('//div[@id="mw-subcategories"]//a/@href').getall():
            url = response.urljoin(href)
            if url not in self.seen_categories:
                yield scrapy.Request(url, callback=self.parse_category)

        # 2) Ссылки на страницы в категории
        for a in response.xpath('//div[@id="mw-pages"]//li/a[starts-with(@href, "/wiki/")]'):
            href = a.xpath("./@href").get()
            title = _clean_text(a.xpath("string(.)").get())

            if not href:
                continue

            # фильтры мусора
            if ":" in href:                       # Категория:, Служебная:, Файл: и т.п.
                continue
            if title.startswith("Список "):       # часто это не фильм
                continue

            url = response.urljoin(href)
            if url in self.seen_films:
                continue

            self.seen_films.add(url)

            # лимит по количеству фильмов (для отладки)
            if self.film_count >= self.max_films:
                return

            yield scrapy.Request(url, callback=self.parse_film)

        # 3) Пагинация категории ("Следующая страница")
        next_href = response.xpath('//a[contains(., "Следующая страница")]/@href').get()
        if next_href:
            yield scrapy.Request(response.urljoin(next_href), callback=self.parse_category)

    # ---------- FILM PAGE PARSING ----------

    def _infobox_td(self, response, labels):
        """
        Находит td в инфобоксе по заголовку th, содержащему один из labels.
        """
        for label in labels:
            td = response.xpath(
                f'//table[contains(@class,"infobox")]//tr[th//text()[contains(., "{label}")]]/td[1]'
            )
            if td and td.get():
                return td
        return None

    def _td_to_value(self, td_sel):
        """
        Превращает td в значение: предпочитаем тексты ссылок (они аккуратнее),
        иначе берём весь текст.
        """
        if td_sel is None:
            return ""

        link_texts = [t.strip() for t in td_sel.xpath('.//a//text()').getall() if t.strip()]
        link_texts = [t for t in link_texts if t not in {"[", "]"}]
        link_texts = _uniq_preserve(link_texts)
        if link_texts:
            return _clean_text("; ".join(link_texts))

        raw = " ".join([t.strip() for t in td_sel.xpath(".//text()").getall() if t.strip()])
        return _clean_text(raw)

    def _extract_year(self, text):
        text = _clean_text(text)
        m = re.search(r"(18|19|20)\d{2}", text)
        return m.group(0) if m else ""

    def parse_film(self, response):
        # базовая проверка: есть ли инфобокс
        infobox = response.xpath('//table[contains(@class,"infobox")]')
        if not infobox or not infobox.get():
            return

        title = _clean_text(response.xpath('string(//h1[@id="firstHeading"])').get())
        wiki_url = response.url

        genre = self._td_to_value(self._infobox_td(response, ["Жанр", "Жанры"]))
        director = self._td_to_value(self._infobox_td(response, ["Режиссёр", "Режиссер", "Режиссёры", "Режиссеры"]))
        country = self._td_to_value(self._infobox_td(response, ["Страна", "Страны"]))

        year_raw = self._td_to_value(self._infobox_td(response, ["Год", "Годы", "Премьера", "Дата выхода"]))
        year = self._extract_year(year_raw)

        item = {
            "title": title,
            "genre": genre,
            "director": director,
            "country": country,
            "year": year,
            "wiki_url": wiki_url,
        }

        # счётчик (факт успешной обработки фильма)
        self.film_count += 1

        # IMDb (опционально): Wikipedia -> Wikidata -> IMDb
        if not self.imdb_enabled:
            yield item
            return

        wikidata_href = response.xpath('//li[@id="t-wikibase"]/a/@href').get()
        if not wikidata_href or "wikidata.org/wiki/" not in wikidata_href:
            # fallback: без imdb
            item["imdb_id"] = ""
            item["imdb_rating"] = ""
            yield item
            return

        qid = wikidata_href.rsplit("/", 1)[-1].split("#")[0].strip()
        wd_json_url = f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json?flavor=dump"

        yield scrapy.Request(
            wd_json_url,
            callback=self.parse_wikidata,
            meta={"item": item, "qid": qid},
        )

    def parse_wikidata(self, response):
        item = response.meta["item"]
        qid = response.meta["qid"]

        try:
            data = json.loads(response.text)
            ent = data["entities"][qid]
            claims = ent.get("claims", {})
            p345 = claims.get("P345", [])
            imdb_id = ""
            if p345:
                imdb_id = p345[0]["mainsnak"]["datavalue"]["value"]
            imdb_id = str(imdb_id).strip()
        except Exception:
            imdb_id = ""

        item["imdb_id"] = imdb_id

        if not imdb_id:
            item["imdb_rating"] = ""
            yield item
            return

        imdb_url = f"https://www.imdb.com/title/{imdb_id}/"
        item["imdb_url"] = imdb_url

        yield scrapy.Request(
            imdb_url,
            callback=self.parse_imdb,
            meta={"item": item},
            headers={"Accept-Language": "en-US,en;q=0.9"},
        )

    def parse_imdb(self, response):
        item = response.meta["item"]

        rating = ""
        # На IMDb часто есть JSON-LD со структурой и aggregateRating
        scripts = response.xpath('//script[@type="application/ld+json"]/text()').getall()
        for s in scripts:
            s = s.strip()
            if not s:
                continue
            try:
                js = json.loads(s)
                if isinstance(js, list):
                    # иногда несколько объектов
                    for obj in js:
                        ar = obj.get("aggregateRating", {}) if isinstance(obj, dict) else {}
                        if ar.get("ratingValue"):
                            rating = str(ar.get("ratingValue"))
                            break
                elif isinstance(js, dict):
                    ar = js.get("aggregateRating", {})
                    if ar.get("ratingValue"):
                        rating = str(ar.get("ratingValue"))
                if rating:
                    break
            except Exception:
                continue

        item["imdb_rating"] = rating
        yield item
