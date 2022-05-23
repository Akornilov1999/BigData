package bdtc.lab2;

import lombok.AllArgsConstructor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

/**
 * CacheData class implements the main set of functions necessary for working with the cache
 */
@AllArgsConstructor
public class CacheData {
    /**
     * Cache name
     */
    private final static String FLIGHTS_CACHE = "flights";

    /**
     * Class for flight with the cache
     */
    private final Ignite ignite;

    /**
     * Datetime formatter
     */
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Regex pattern to split input flights row
     */
    private final static Pattern splitStr = Pattern.compile(",");

    /**
     * Method, which return cache configuration
     * Uses flights class {@link Flights}
     * @return cache configuration
     */
    public static CacheConfiguration<UUID, Flights> getCacheDataConfig() {
        CacheConfiguration<UUID, Flights> cacheConfig = new CacheConfiguration<>(FLIGHTS_CACHE);
        cacheConfig.setStatisticsEnabled(true);

        cacheConfig.setIndexedTypes(UUID.class, Flights.class);

        cacheConfig.setCacheMode(CacheMode.PARTITIONED);
        cacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cacheConfig.setQueryEntities(F.asList(
                new QueryEntity()
                        .setKeyType(UUID.class.getName())
                        .setValueType(Flights.class.getName())
                        .setTableName("Flights")
                        .addQueryField("number", String.class.getName(), "NUMBER")
                        .addQueryField("dateTimeFrom", String.class.getName(), "DATETIMEFROM")
                        .addQueryField("airportFrom", String.class.getName(), "AIRPORTFROM")
                        .addQueryField("airportTo", String.class.getName(), "AIRPORTTO"),
                new QueryEntity()
                        .setKeyType(UUID.class.getName())
                        .setValueType(AirportCountry.class.getName())
                        .setTableName("AirportCountry")
                        .addQueryField("airport", String.class.getName(), "AIRPORT")
                        .addQueryField("country", String.class.getName(), "COUNTRY")));
        return cacheConfig;
    }

    /**
     * Method, which load flights data in cache from list
     * param path to file with data
     * @throws IOException from FileReader()
     */
    public void loadData(List<String> data) throws IOException {
        ignite.getOrCreateCache(getCacheDataConfig());
        IgniteDataStreamer<UUID, Flights> streamer = ignite.dataStreamer(FLIGHTS_CACHE);
        String[] splitData;
        for (int i = 0; i < data.size(); i++) {
            splitData = splitStr.split(data.get(i));
            if (splitData.length != 4) throw new RuntimeException("Invalid number of columns in data line");
            Flights f = new Flights(splitData[0],
                    splitData[1], splitData[2], splitData[3]);
            streamer.addData(UUID.randomUUID(), f);
        }
        streamer.close();
    }

    /**
     * Method, which load mapper (airportCountry) data in cache from list
     * param path to file with mapper
     * @throws IOException from FileReader()
     */
    public void loadMapper(Map<String,String> mapper) throws IOException {
        ignite.getOrCreateCache(getCacheDataConfig());
        IgniteDataStreamer<UUID, AirportCountry> streamer = ignite.dataStreamer(FLIGHTS_CACHE);
        Object[] keys = mapper.keySet().toArray();
        for (int i = 0; i < mapper.size(); i++) {
            AirportCountry ac = new AirportCountry((String)keys[i], mapper.get(keys[i]));
            streamer.addData(UUID.randomUUID(), ac);
        }
        streamer.close();
    }

    /**
     * Method, which convert and put in cache row
     * @param row input string
     * @return id data in cache
     * @throws RuntimeException if the input string is invalid
     */
    public UUID put(String row) throws RuntimeException {
        IgniteCache<UUID, Flights> cache =  ignite.getOrCreateCache(getCacheDataConfig());
        String[] splitData = splitStr.split(row);
        if (splitData.length != 4) throw new RuntimeException("Invalid number of columns in data line");
        Flights inp = new Flights(splitData[0],
                splitData[1], splitData[2], splitData[3]);
        UUID uuid = UUID.randomUUID();
        cache.put(uuid, inp);
        return uuid;
    }

    /**
     * Method, which delete data from cache by id
     * @param id row in cache
     */
    public void delete(UUID id) {
        IgniteCache<UUID, Flights> cache =  ignite.getOrCreateCache(getCacheDataConfig());
        cache.remove(id);
    }
}