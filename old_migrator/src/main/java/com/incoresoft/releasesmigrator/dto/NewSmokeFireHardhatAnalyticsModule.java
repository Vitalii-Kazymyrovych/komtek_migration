package com.incoresoft.releasesmigrator.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

/**
 * Analytics module for hardhat and smoke fire for 25.1.
 */
@With
@Value
@Jacksonized
@Builder
@AllArgsConstructor
public class NewSmokeFireHardhatAnalyticsModule {
    NewSmokeFireHardhatAdvancedSettings advancedSettings;

    JsonNode hardwareSettings;
    JsonNode polygons;

    public NewSmokeFireHardhatAnalyticsModule(OldSmokeFireHardhatAnalyticsModule oldModule) {
        this.advancedSettings = new NewSmokeFireHardhatAdvancedSettings(oldModule.getAdvancedSettings(), oldModule.getAlertDelay());
        this.hardwareSettings = oldModule.getHardwareSettings();
        this.polygons = oldModule.getPolygons();
    }
}
