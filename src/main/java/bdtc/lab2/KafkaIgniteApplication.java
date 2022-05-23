package bdtc.lab2;

import lombok.extern.log4j.Log4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Ignite application entry point class
 */
@Log4j
public class KafkaIgniteApplication {
    public static void main(String[] args) throws IOException {
        Ignition.setClientMode(true);
        KafkaService kafkaService = new KafkaService();
        kafkaService.connect();
        Ignite ignite = Ignition.start("config/ignite-config.xml");
        CacheData cacheData =  new CacheData(ignite);
        try {
            cacheData.loadData(kafkaService.readAllData());
            cacheData.loadMapper(KafkaService.getAirportCountry());
        } catch (IOException e) {
            e.printStackTrace();
        }
        MyCompute compute = new MyCompute(ignite);
        log.info("start compute");
        Map<CustomKey, String> result = compute.getResults();
        FileWriter wr;
        File output = new File("output.txt");
        if (output.createNewFile()) {
            wr = new FileWriter(output);
            log.info("create output file");
        } else {
            wr = new FileWriter(output);
            wr.flush();
            log.info("open output file");
        }
        result.forEach((key, value) -> {
            try {
                wr.write("{ "+key.getDateTimeFrom()+", "+key.getCountryFrom()+", "+key.getCountryTo()
                        +" } - "+value+"\n");
            } catch (IOException e) {
                log.error("unable to write result");
                e.printStackTrace();
            }
        });
        wr.close();
        ignite.close();
    }
}