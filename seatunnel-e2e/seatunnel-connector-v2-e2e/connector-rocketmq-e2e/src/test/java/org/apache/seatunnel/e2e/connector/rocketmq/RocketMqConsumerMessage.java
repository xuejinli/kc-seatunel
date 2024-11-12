package org.apache.seatunnel.e2e.connector.rocketmq;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class RocketMqConsumerMessage {
    private String value;
    private String tag;
}
