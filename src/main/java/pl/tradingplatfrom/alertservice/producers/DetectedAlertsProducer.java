package pl.tradingplatfrom.alertservice.producers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import pl.tradingplatform.events.AlertEvent;
import pl.tradingplatform.events.StockPriceEvent;

import java.time.LocalDateTime;

@Slf4j
@RequiredArgsConstructor
@Service
public class DetectedAlertsProducer {

    private final KafkaTemplate<String, AlertEvent> kafkaTemplate;

    public void sendDetectedAlert(StockPriceEvent stockPriceEvent, double change) {
        log.info("Sending alert for {} with {}", stockPriceEvent.getTicker(), change);
        AlertEvent alertEvent = new AlertEvent(stockPriceEvent.getTicker(),
                "Price drop: %.2f".formatted(change) + "%",
                stockPriceEvent.getCurrentPrice(),
                LocalDateTime.now());
        kafkaTemplate.send("detected-alerts", stockPriceEvent.getTicker(), alertEvent);
    }

}
