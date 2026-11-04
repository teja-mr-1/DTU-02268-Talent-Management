package dk.dtu.applications.model;

import java.util.Objects;

public record IntakeEvent<T>(EventType type, T payload) {
    public IntakeEvent {
        Objects.requireNonNull(type);
        Objects.requireNonNull(payload);
        if (!type.payloadType().isInstance(payload)) {
            throw new IllegalArgumentException("Incorrect payload for " + type);
        }
    }
}
