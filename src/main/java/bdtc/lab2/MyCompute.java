package bdtc.lab2;

import lombok.AllArgsConstructor;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;

/**
 * MyCompute class, that compute the results from the cache
 */
@AllArgsConstructor
public class MyCompute {
    /**
     * Class for flight with the cache
     */
    private final Ignite ignite;

    /**
     * Method, that get ignite cache
     * Uses flights class {@link Flights}
     * @return ignite cache
     */
    private IgniteCache<UUID, Flights> flCache() {
        CacheConfiguration<UUID, Flights> cacheConf = CacheData.getCacheDataConfig();
        return ignite.getOrCreateCache(cacheConf);
    }

    /**
     * Method, that compute the results
     * Uses customKey class {@link CustomKey}
     * @return results, consisting of a key and a quantity
     */
    public Map<CustomKey, String> getResults() {
        IgniteCache<UUID, Flights> cache = flCache();
        SqlFieldsQuery query = new SqlFieldsQuery(
                "SELECT DATE_TRUNC('HOUR', f.DateTimeFrom), ac1.Country, ac2.Country, count(*) " +
                        "FROM Flights as f, AirportCountry as ac1, AirportCountry as ac2 " +
                        "WHERE f.airportFrom = ac1.airport and f.airportTo = ac2.airport " +
                        "GROUP BY (DATE_TRUNC('HOUR', f.DateTimeFrom), ac1.Country, ac2.Country)")
                .setEnforceJoinOrder(true);
        Map<CustomKey, String> result = new HashMap<>();
        QueryCursor<List<?>> cursor = cache.query(query);
        for (List<?> row : cursor) {
            CustomKey key = new CustomKey(row.get(0).toString().substring(0, 19), (String) row.get(1), (String) row.get(2));
            result.put(key, row.get(3).toString());
        }
        return result;
    }
}