package com.hunglp.producer.api;

import com.hunglp.producer.dto.UserClickEvent;
import com.hunglp.producer.dto.UserPurchaseEvent;
import com.hunglp.producer.dto.UserViewEvent;
import com.hunglp.producer.service.AnalyticsEventService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsEventService analyticsEventService;

    @PostMapping("/events/click")
    public ResponseEntity<String> trackClick(@RequestBody UserClickEvent event) {
        analyticsEventService.sendClickEvent(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("Click event tracked");
    }

    @PostMapping("/events/view")
    public ResponseEntity<String> trackView(@RequestBody UserViewEvent event) {
        analyticsEventService.sendViewEvent(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("View event tracked");
    }

    @PostMapping("/events/purchase")
    public ResponseEntity<String> trackPurchase(@RequestBody UserPurchaseEvent event) {
        analyticsEventService.sendPurchaseEvent(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("Purchase event tracked");
    }

    // Demo endpoints to generate sample events
    @PostMapping("/demo/generate")
    public ResponseEntity<String> generateDemoEvents(@RequestParam(defaultValue = "10") int count) {
        String sessionId = UUID.randomUUID().toString();
        
        for (int i = 0; i < count; i++) {
            String userId = "user" + (i % 5 + 1);
            
            // Generate click event
            UserClickEvent clickEvent = new UserClickEvent(
                    userId, sessionId,
                    "/products/laptop-" + i,
                    "btn-add-to-cart",
                    "button",
                    "/products"
            );
            analyticsEventService.sendClickEvent(clickEvent);
            
            // Generate view event
            UserViewEvent viewEvent = new UserViewEvent(
                    userId, sessionId,
                    "/products/laptop-" + i,
                    "Laptop Product Page",
                    5000L + (i * 100),
                    "/products"
            );
            analyticsEventService.sendViewEvent(viewEvent);
            
            // Generate purchase event (every 3rd event)
            if (i % 3 == 0) {
                UserPurchaseEvent purchaseEvent = new UserPurchaseEvent(
                        userId, sessionId,
                        "order-" + UUID.randomUUID().toString(),
                        List.of(new UserPurchaseEvent.PurchaseItem(
                                "prod-" + i,
                                "Laptop " + i,
                                1,
                                BigDecimal.valueOf(1000 + i * 100)
                        )),
                        BigDecimal.valueOf(1000 + i * 100),
                        "CREDIT_CARD",
                        "USD"
                );
                analyticsEventService.sendPurchaseEvent(purchaseEvent);
            }
        }
        
        return ResponseEntity.ok("Generated " + count + " demo events");
    }
}
