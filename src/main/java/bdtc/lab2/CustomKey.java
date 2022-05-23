package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.time.LocalDateTime;
import java.io.Serializable;

/**
 * Serializable data class for output key {@link Serializable}
 */
@AllArgsConstructor
@Data
public class CustomKey implements Serializable {
    /**
     * DateTimeFrom
     */
    private String dateTimeFrom;
    /**
     * Airport name
     */
    private String countryFrom;

    /**
     * Country name
     */
    private String countryTo;
}