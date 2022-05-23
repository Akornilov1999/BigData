# -*- coding: utf-8 -*-

import random
import time
from kafka import KafkaProducer

dict_airport = {'Аэропорт Вильгельмсхафен':'Германия',
'Аэропорт Гибралтар':'Великобритания',
'Аэропорт Глазго Прествик':'Великобритания',
'Аэропорт Голуэй':'Ирландия',
'Аэропорт Дублин':'Ирландия',
'Аэропорт Гронинген Элде':'Нидерланды',
'Аэропорт Быдгощ Игнацы Ян Падеревский':'Польша',
'Аэропорт Вагар':'Фарерские острова',
'Аэропорт Вангерооге':'Германия',
'Аэропорт Варшава Модлин':'Польша',
'Аэропорт Варна':'Болгария',
'Аэропорт Векшё Смоланд':'Швеция',
'Аэропорт Венеция Марко Поло':'Италия',
'Аэропорт Вентспилс':'Латвия',
'Аэропорт Веце':'Германия',
'Аэропорт Ираклион Никос Казантзакис':'Греция',
'Аэропорт Осло Гардермуэн':'Норвегия',
'Аэропорт Гданьск Лех Валенса':'Польша',
'Аэропорт Каунас':'Литва',
'Аэропорт Бухарест Генри Коандэ':'Румыния',
'Аэропорт Вильнюс':'Литва',
'Аэропорт Хельсинки Вантаа':'Финляндия',
'Аэропорт Пафос':'Кипр',
'Аэропорт Милан Мальпенса':'Италия',
'Аэропорт Афины Элефтериос Венизелос':'Греция',
'Аэропорт Таллин Леннарт Мэри':'Эстония',
'Аэропорт Москва Шереметьево':'Россия',
'Аэропорт Ларнака':'Кипр',
'Аэропорт Мюнхен':'Германия',
'Аэропорт Рим Леонардо да Винчи - Фьюмичино':'Италия',
'Аэропорт Бургас':'Болгария',
'Аэропорт Москва Внуково':'Россия',
'Аэропорт Вена Швехат':'Австрия',
'Аэропорт Альтенрайн Санкт-Галлен':'Швейцария',
'Аэропорт Аоста Коррадо Гекс':'Италия',
'Аэропорт Альгеро Фертилия':'Италия',
'Аэропорт Амстердам Схипхол':'Нидерланды',
'Аэропорт Анталья':'Турция',
'Аэропорт Брауншвейг-Вольфсбург':'Германия',
'Аэропорт Брешиа Монтикьяри':'Италия',
'Аэропорт Будапешт Ференц Лист':'Венгрия',
'Аэропорт Бургос':'Испания',
'Аэропорт Гранада Федерико Гарсия Лорка':'Испания',
'Аэропорт Белфаст-Сити Джордж Бест':'Великобритания',
'Аэропорт Лондон Гатвик':'Великобритания',
'Аэропорт Мадрид Таррагона':'Испания',
'Аэропорт Ле Туке Опаловый берег':'Франция',
'Аэропорт Эдинбург':'Великобритания',
'Аэропорт Мадрид Барахас':'Испания',
'Аэропорт Клермон-Ферран Овернь':'Франция',
'Аэропорт Белград Никола Тесла':'Сербия',
'Аэропорт Берн Бельп':'Швейцария',
'Аэропорт Закинтос Дионисий Соломос':'Греция',
'Аэропорт Задар':'Хорватия',
'Аэропорт Измир Аднан Мендерес':'Турция',
'Аэропорт Москва Домодедово':'Россия'}

def str_time_prop(start, end, format, prop):
	"""Get a time at a proportion of a range of two formatted times.

	start and end should be strings specifying times formated in the
	given format (strftime-style), giving an interval [start, end].
	prop specifies how a proportion of the interval to be taken after
	start.  The returned time will be in the specified format.
	"""

	stime = time.mktime(time.strptime(start, format))
	etime = time.mktime(time.strptime(end, format))

	ptime = stime + prop * (etime - stime)

	return time.strftime(format, time.localtime(ptime))

def random_date(start, end, prop):
	return str_time_prop(start, end, '%Y-%m-%d %H:%M:%S', prop)

def flight_date():
	start_random = random.random()
	from_date = random_date("2022-1-1 0:0:0", "2022-12-31 23:59:59", start_random)
	return from_date

bootstrap_servers = ['localhost:9092']
topicName = 'My-stream'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()

arr_airport = list(dict_airport.keys())
flight_number = random.randint(100, 1000)
print('Количество генерируемых данных:')
randomNumber = 0
randomNumberGlobal = 0
randomDateGlobal = ''
n = int(input())
for i in range(n):
	flight_letter = random.choice('ABCEFHKLMNPRSTVXY')
	flight_number += random.randint(1, 25)
	randomNumberLocal = random.randint(0, 55)
	while randomNumberGlobal == randomNumberLocal:
    	    randomNumberLocal = random.randint(0, 55)
	randomNumber += randomNumberLocal
	from_key = arr_airport[randomNumber % 54]#random.choice(arr_airport)
	randomNumberGlobal = randomNumberLocal
	from_airport = str(from_key)#+','+str(dict_airport[from_key])

	randomNumberLocal = random.randint(0, 55)
	while randomNumberGlobal == randomNumberLocal:
	    randomNumberLocal = random.randint(0, 55)
	randomNumber += randomNumberLocal
	to_key = arr_airport[randomNumber % 54]#random.choice(arr_airport)
	randomNumberGlobal = randomNumberLocal
	to_airport = str(to_key)#+','+str(dict_airport[to_key])

	randomDateLocal = flight_date()
    	while randomDateGlobal == randomDateLocal:
        	randomDateLocal = random.randint(0, 55)
	flight_str = flight_letter+str(flight_number)+','+randomDateLocal+','+from_airport+','+to_airport
	randomDateGlobal = randomDateLocal
	producer.send(topicName, bytes(flight_str))
	producer.flush()

producer.close()
