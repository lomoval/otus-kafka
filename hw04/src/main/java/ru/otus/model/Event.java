package ru.otus.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

@Builder(toBuilder = true)
@Value
@AllArgsConstructor
public class Event {
    @JsonProperty
    String key;
    @JsonProperty
    String userId;
    @JsonProperty
    Integer value;

    public static EventBuilder from(Event event){
        return builder()
                .key(event.getKey())
                .userId(event.getUserId())
                .value(event.getValue());
    }
}
