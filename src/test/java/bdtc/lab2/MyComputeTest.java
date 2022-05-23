package bdtc.lab2;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MyComputeTest{
    private static CacheData cacheData;
    private static MyCompute compute;
    private final Map<CustomKey, String> baseResult = new HashMap<>();

    @BeforeClass
    public static void init(){
        Ignite ignite = Ignition.start("config/ignite-config.xml");
        cacheData =  new CacheData(ignite);
        try {
            cacheData.loadMapper(KafkaService.getAirportCountry());
        } catch (IOException e) {
            e.printStackTrace();
        }
        compute = new MyCompute(ignite);
    }

    @Test
    public void testCountFlightEqualAirport(){
        UUID id1 = cacheData.put("A0001,2022-01-23 15:14:12,Аэропорт Гибралтар,Аэропорт Голуэй");
        UUID id2 = cacheData.put("A0001,2022-01-23 15:24:12,Аэропорт Гибралтар,Аэропорт Голуэй");
        UUID id3 = cacheData.put("A0001,2022-01-23 16:24:12,Аэропорт Гибралтар,Аэропорт Голуэй");
        UUID id4 = cacheData.put("A0001,2022-02-23 15:24:12,Аэропорт Гибралтар,Аэропорт Голуэй");
        baseResult.put(new CustomKey("2022-02-23 15:00:00","Великобритания",
                "Ирландия"),"1");
        baseResult.put(new CustomKey("2022-01-23 16:00:00","Великобритания",
                "Ирландия"),"1");
        baseResult.put(new CustomKey("2022-01-23 15:00:00","Великобритания",
                "Ирландия"),"2");
        assert baseResult.equals(compute.getResults());
        baseResult.replace(new CustomKey("2022-02-23 15:00:00","Великобритания",
                "Ирландия"),"1");
        baseResult.replace(new CustomKey("2022-01-23 16:00:00","Великобритания",
                "Ирландия"),"1");
        baseResult.replace(new CustomKey("2022-01-23 15:00:00","Великобритания",
                "Ирландия"),"2");
        cacheData.delete(id1);
        cacheData.delete(id2);
        cacheData.delete(id3);
        cacheData.delete(id4);
    }

    @Test
    public void testCountFlightEquallyCountry(){
        UUID id1 = cacheData.put("A0001,2022-01-23 15:14:12,Аэропорт Гибралтар,Аэропорт Голуэй");
        UUID id2 = cacheData.put("A0001,2022-01-23 15:17:12,Аэропорт Глазго Прествик,Аэропорт Голуэй");
        UUID id3 = cacheData.put("A0001,2022-01-23 15:47:12,Аэропорт Глазго Прествик,Аэропорт Дублин");
        UUID id4 = cacheData.put("A0001,2022-01-23 15:18:12,Аэропорт Гибралтар,Аэропорт Дублин");
        UUID id5 = cacheData.put("A0001,2022-01-23 16:18:12,Аэропорт Гибралтар,Аэропорт Дублин");
        UUID id6 = cacheData.put("A0001,2022-02-23 15:18:12,Аэропорт Гибралтар,Аэропорт Дублин");
        baseResult.put(new CustomKey("2022-01-23 15:00:00","Великобритания",
                "Ирландия"),"4");
        baseResult.put(new CustomKey("2022-02-23 15:00:00","Великобритания",
                "Ирландия"),"1");
        baseResult.put(new CustomKey("2022-01-23 16:00:00","Великобритания",
                "Ирландия"),"1");
        assert baseResult.equals(compute.getResults());
        baseResult.replace(new CustomKey("2022-01-23 15:00:00","Великобритания",
                "Ирландия"),"4");
        baseResult.replace(new CustomKey("2022-02-23 15:00:00","Великобритания",
                "Ирландия"),"1");
        baseResult.replace(new CustomKey("2022-01-23 16:00:00","Великобритания",
                "Ирландия"),"1");
        cacheData.delete(id1);
        cacheData.delete(id2);
        cacheData.delete(id3);
        cacheData.delete(id4);
        cacheData.delete(id5);
        cacheData.delete(id6);
    }

    @Test
    public void testCountFlight()
    {
        UUID id1 = cacheData.put("A0001,2022-01-23 15:00:00,Аэропорт Гибралтар,Аэропорт Голуэй");
        UUID id2 = cacheData.put("A0001,2022-01-23 15:00:00,Аэропорт Глазго Прествик,Аэропорт Задар");
        UUID id3 = cacheData.put("A0001,2022-01-23 15:00:00,Аэропорт Задар,Аэропорт Гибралтар");
        UUID id4 = cacheData.put("A0001,2022-01-23 15:00:00,Аэропорт Будапешт Ференц Лист,Аэропорт Берн Бельп");
        baseResult.put(new CustomKey("2022-01-23 15:00:00","Великобритания",
                "Ирландия"),"1");
        baseResult.put(new CustomKey("2022-01-23 15:00:00","Великобритания",
                "Хорватия"),"1");
        baseResult.put(new CustomKey("2022-01-23 15:00:00","Хорватия",
                "Великобритания"),"1");
        baseResult.put(new CustomKey("2022-01-23 15:00:00","Венгрия",
                "Швейцария"),"1");
        assert baseResult.equals(compute.getResults());
        baseResult.replace(new CustomKey("2022-01-23 15:00:00","Великобритания",
                "Ирландия"),"1");
        baseResult.replace(new CustomKey("2022-01-23 15:00:00","Великобритания",
                "Хорватия"),"1");
        baseResult.replace(new CustomKey("2022-01-23 15:00:00","Хорватия",
                "Великобритания"),"1");
        baseResult.replace(new CustomKey("2022-01-23 15:00:00","Венгрия",
                "Швейцария"),"1");
        cacheData.delete(id1);
        cacheData.delete(id2);
        cacheData.delete(id3);
        cacheData.delete(id4);
    }
}