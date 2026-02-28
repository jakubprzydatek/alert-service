package pl.tradingplatfrom.alertservice.listeners;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import pl.tradingplatform.events.StockPriceEvent;
import pl.tradingplatfrom.alertservice.producers.DetectedAlertsProducer;

@Service
@Slf4j
@RequiredArgsConstructor
public class AlertListener {

    private static final double ALERT_THRESHOLD = -0.5;
    private final DetectedAlertsProducer detectedAlertsProducer;

    @KafkaListener(topics = "market-prices", groupId = "alert-monitor-group")
    public void processMarketData(StockPriceEvent event) {
        double changePercent = calculateChange(event.getPreviousPrice(), event.getCurrentPrice());

        if (changePercent <= ALERT_THRESHOLD) {
            sendAlert(event, changePercent);
        } else {
            log.info("Price stable for {}: {}%", event.getTicker(), String.format("%.2f", changePercent));
        }
    }

    private double calculateChange(double oldPrice, double newPrice) {
        return ((newPrice - oldPrice) / oldPrice) * 100;
    }

    private void sendAlert(StockPriceEvent event, double change) {
        log.error("🚨 ALERT! 🚨 {} price dropped by {}%! Current: {}, Previous: {}",
                event.getTicker(), String.format("%.2f", change),
                event.getCurrentPrice(), event.getPreviousPrice());

        detectedAlertsProducer.sendDetectedAlert(event, change);
    }

}
