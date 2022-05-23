package bdtc.lab2;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class KafkaService
{
    private static final Map<String, String> airport_country = new HashMap<>();
    static {
        airport_country.put("Аэропорт Вильгельмсхафен","Германия");
        airport_country.put("Аэропорт Гибралтар","Великобритания");
        airport_country.put("Аэропорт Глазго Прествик","Великобритания");
        airport_country.put("Аэропорт Голуэй","Ирландия");
        airport_country.put("Аэропорт Дублин","Ирландия");
        airport_country.put("Аэропорт Гронинген Элде","Нидерланды");
        airport_country.put("Аэропорт Быдгощ Игнацы Ян Падеревский","Польша");
        airport_country.put("Аэропорт Вагар","Фарерские острова");
        airport_country.put("Аэропорт Вангерооге","Германия");
        airport_country.put("Аэропорт Варшава Модлин","Польша");
        airport_country.put("Аэропорт Варна","Болгария");
        airport_country.put("Аэропорт Векшё Смоланд","Швеция");
        airport_country.put("Аэропорт Венеция Марко Поло","Италия");
        airport_country.put("Аэропорт Вентспилс","Латвия");
        airport_country.put("Аэропорт Веце","Германия");
        airport_country.put("Аэропорт Ираклион Никос Казантзакис","Греция");
        airport_country.put("Аэропорт Осло Гардермуэн","Норвегия");
        airport_country.put("Аэропорт Гданьск Лех Валенса","Польша");
        airport_country.put("Аэропорт Каунас","Литва");
        airport_country.put("Аэропорт Бухарест Генри Коандэ","Румыния");
        airport_country.put("Аэропорт Вильнюс","Литва");
        airport_country.put("Аэропорт Хельсинки Вантаа","Финляндия");
        airport_country.put("Аэропорт Пафос","Кипр");
        airport_country.put("Аэропорт Милан Мальпенса","Италия");
        airport_country.put("Аэропорт Афины Элефтериос Венизелос","Греция");
        airport_country.put("Аэропорт Таллин Леннарт Мэри","Эстония");
        airport_country.put("Аэропорт Москва Шереметьево","Россия");
        airport_country.put("Аэропорт Ларнака","Кипр");
        airport_country.put("Аэропорт Мюнхен","Германия");
        airport_country.put("Аэропорт Рим Леонардо да Винчи - Фьюмичино","Италия");
        airport_country.put("Аэропорт Бургас","Болгария");
        airport_country.put("Аэропорт Москва Внуково","Россия");
        airport_country.put("Аэропорт Вена Швехат","Австрия");
        airport_country.put("Аэропорт Альтенрайн Санкт-Галлен","Швейцария");
        airport_country.put("Аэропорт Аоста Коррадо Гекс","Италия");
        airport_country.put("Аэропорт Альгеро Фертилия","Италия");
        airport_country.put("Аэропорт Амстердам Схипхол","Нидерланды");
        airport_country.put("Аэропорт Анталья","Турция");
        airport_country.put("Аэропорт Брауншвейг-Вольфсбург","Германия");
        airport_country.put("Аэропорт Брешиа Монтикьяри","Италия");
        airport_country.put("Аэропорт Будапешт Ференц Лист","Венгрия");
        airport_country.put("Аэропорт Бургос","Испания");
        airport_country.put("Аэропорт Гранада Федерико Гарсия Лорка","Испания");
        airport_country.put("Аэропорт Белфаст-Сити Джордж Бест","Великобритания");
        airport_country.put("Аэропорт Лондон Гатвик","Великобритания");
        airport_country.put("Аэропорт Мадрид Таррагона","Испания");
        airport_country.put("Аэропорт Ле Туке Опаловый берег","Франция");
        airport_country.put("Аэропорт Эдинбург","Великобритания");
        airport_country.put("Аэропорт Мадрид Барахас","Испания");
        airport_country.put("Аэропорт Клермон-Ферран Овернь","Франция");
        airport_country.put("Аэропорт Белград Никола Тесла","Сербия");
        airport_country.put("Аэропорт Берн Бельп","Швейцария");
        airport_country.put("Аэропорт Закинтос Дионисий Соломос","Греция");
        airport_country.put("Аэропорт Задар","Хорватия");
        airport_country.put("Аэропорт Измир Аднан Мендерес","Турция");
        airport_country.put("Аэропорт Москва Домодедово","Россия");
    }

    private static final int STOPLOOP = 5;
    private static final String TOPIC = "My-stream";
    private Consumer<String, String> consumer;
    private String server = "localhost:9092";
    private Integer batchSize = 100;

    public static Map<String,String> getAirportCountry(){
        return airport_country;
    }

    /**
     * Метод инициализации необходимых параметров
     */
    public void connect()
    {
        Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", server);
        config.put("client.id", "testclientid");
        config.put("group.id", "testgroupid");
        config.put("batch.size", batchSize);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(config);
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        topicPartitionList.add(new TopicPartition(TOPIC, 0));
        consumer.assign(topicPartitionList);
        consumer.seekToBeginning(topicPartitionList);
    }

    /**
     * чтение данных и синхронное отмечание факта прочитанности строки
     */
    private List<String> readData() {
        List<String> values = new ArrayList<>();
        consumer
                .poll(100)
                .iterator()
                .forEachRemaining(consumerRecord -> values
                        .add(consumerRecord.value()));
        consumer.commitSync();
        return values;
    }

    /**
     * цикл, который читает данные несколько раз и завершается, если STOPLOOP раз ничего не было прочитано
     */
    public List<String> readAllData()
    {
        List<String> data = new ArrayList<>();

        for(int i=0; i<STOPLOOP; i++)
        {
            List<String> strings = readData();
            if(strings.size()>0)
            {
                i=0;
                data.addAll(strings);
            }

        }
        return data;
    }
}