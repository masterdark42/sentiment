from flask import Flask, g, request
import sqlite3
import json
from datetime import datetime, UTC
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)
DATABASE = 'reviews.db'
DEBUG = True
HTTP_BAD_REQUEST = 400

# TODO: подумать над сменой типа created_at на datetime
SQL_INIT_DB = ('''
                CREATE TABLE IF NOT EXISTS reviews (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  text TEXT NOT NULL,
                  sentiment TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  CHECK (sentiment in ('positive', 'negative', 'neutral'))
                ); ''',
               'CREATE INDEX IF NOT EXISTS idx_reviews_sent ON reviews (sentiment);'
              )

SQL_GET_REVIEWS = 'SELECT id, sentiment, text, created_at FROM reviews {} ORDER BY id'
SQL_GET_REVIEWS_WHERE = 'WHERE sentiment = :sentiment'

def get_db(get_cursor: bool = True):
    """Получение коннекта и курсора БД

    :param get_cursor: флаг получения курсора, по умолчанию - True
    :return: коннект, курсор"""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db, db.cursor() if get_cursor else None

@app.teardown_appcontext
def close_connection(exception):
    """Отключение от БД"""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    """Инициализация БД"""
    try:
        with app.app_context():
            db, cursor = get_db()
            for sql in SQL_INIT_DB:
                cursor.execute(sql)
            logger.info('БД проинициализирована')
    except Exception as e:
        logger.fatal(str(e), exc_info=True)

def sentiment_comment(review: str):
    """Определение оценки комментария на основе вхождения позитивно/негативно окрашенных слов

    :param review: строка с комментарием
    :return: строка с оценкой: positive/negative/neutral
    """

    positive_words = ('хорош', 'люблю', 'любим', 'отличн', 'обожа', 'прекрасн', 'замечательн', 'классн', 'кайф', 'приятн')
    negative_words = ('плох', 'ужас', 'ненавиж', 'отвра', 'отстой')
    positive_count, negative_count = 0, 0

    # Разделяем комментарий на слова в нижнем регистре
    words = review.lower().split()

    # Ищем первое вхождение окрашенного слова
    for i, word in enumerate(words):
        if any(word.startswith(positive_word) for positive_word in positive_words):
            # Проверяем, есть ли перед словом отрицательная частица/приставка
            if word[:2] == 'не' or (i > 0 and words[i - 1] == 'не'):
                negative_count += 1
            else:
                positive_count += 1
        elif any(word.startswith(negative_word) for negative_word in negative_words):
            # Проверяем, есть ли перед словом отрицательная частица или приставка
            if word[:2] == 'не' or (i > 0 and words[i - 1] == 'не'):
                positive_count += 1
            else:
                negative_count += 1

    # Определяем общую оценку комментария
    if positive_count > negative_count:
        return 'positive'
    elif negative_count > positive_count:
        return 'negative'
    else:
        return 'neutral'

@app.route('/reviews', methods=['POST'])
def add_review():
    """Добавление отзыва

    :return: JSON с описанием отзыва: {id, sentiment, text, created_at}"""
    try:
        req = request.json
        if 'text' not in req:
            raise Exception('В запросе отсутствует атрибут text')
        # TODO: подумать насчет ограничения max длины комментария
        review = req['text'].strip()
        review_dict = {'text': review,
                       'sentiment': sentiment_comment(review),
                       'created_at': datetime.now(UTC).isoformat()
                      }
        # Сохраняем комментарий в БД и возвращаем всю информацию о нем с учетом нового ID
        db, cursor = get_db()
        # TODO: Подумать насчет проверки комментария на дублирование за последние N минут
        review_dict['id'] = cursor.execute("INSERT INTO reviews (text, sentiment, created_at) VALUES (:text, :sentiment, :created_at)",
                                           review_dict).lastrowid
        db.commit()
        return json.dumps(review_dict)
    except Exception as e:
        logger.error(str(e), exc_info=True)
        return json.dumps({'error': str(e)}), HTTP_BAD_REQUEST

# Маршрут для получения комментариев
@app.route('/reviews', methods=['GET'])
def get_reviews():
    """Роут получения отзывов по передаваемому параметру sentiment

    :return: JSON с массивом отзывов [{id, sentiment, text, created_at}]"""
    try:
        sentiment = request.args.get('sentiment', '').strip().lower()
        if sentiment not in ('positive', 'negative', 'neutral', ''):
            raise Exception('Атрибут sentiment имеет недопустимое значение')

        # Если sentiment не указан явно, выводим все комментарии
        where, params = (SQL_GET_REVIEWS_WHERE, (sentiment,)) if sentiment else ('', ())

        db, cursor = get_db()
        cursor.execute(SQL_GET_REVIEWS.format(where), params)

        reviews = []
        # TODO: Подумать насчет лимитирования кол-ва отдаваемых комментариев
        for row in cursor:
            reviews.append({'id': row[0], 'sentiment': row[1], 'text': row[2], 'created_at': row[3]})
        return reviews
    except Exception as e:
        logger.error(str(e), exc_info=True)
        return json.dumps({'error': str(e)}), HTTP_BAD_REQUEST

if __name__ == '__main__':
    init_db()
    app.run(debug=False)



