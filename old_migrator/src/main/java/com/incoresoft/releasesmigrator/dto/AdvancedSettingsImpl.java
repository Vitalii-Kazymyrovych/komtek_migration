package com.incoresoft.releasesmigrator.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

/**
 * Copied from middleware-analytics-lib: {@code com.incoresoft.middleware.analytics.models.settings.AdvancedSettingsImpl}.
 */
@With
@Value
@Jacksonized
@Builder
public class AdvancedSettingsImpl {
    float sensitivity;
    String model;
    String tracker;
    int trackerBufferTime;
    boolean alarmFiltration;
    int minHeight;
    int minWidth;

    @JsonProperty(value = "trackerSensitivity")
    public int getTrackerSensitivity() {
        return 8;
    }
}