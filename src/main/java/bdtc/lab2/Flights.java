package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Serializable data class for input interactions {@link Serializable}
 */

@AllArgsConstructor
@Data
public class Flights implements Serializable {
    /**
     * Flight number
     */
    @QuerySqlField
    private String number;

    /**
     * DateTimeFrom
     */
    @QuerySqlField
    private String dateTimeFrom;

    /**
     * AirportFrom identifier
     */
    @QuerySqlField
    private String airportFrom;

    /**
     * AirportTo identifier
     */
    @QuerySqlField
    private String airportTo;
}