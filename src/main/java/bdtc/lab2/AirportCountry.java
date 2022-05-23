package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import java.io.Serializable;

/**
 * Serializable data class for output key {@link Serializable}
 */
@AllArgsConstructor
@Data
public class AirportCountry implements Serializable {
    /**
     * Airport name
     */
    @QuerySqlField
    private String Airport;

    /**
     * Country name
     */
    @QuerySqlField
    private String Country;
}
