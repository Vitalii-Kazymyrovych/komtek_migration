package com.incoresoft.releasesmigrator.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

/**
 * Analytics module for hardhat and smoke fire for 23.3.
 */
@With
@Value
@Jacksonized
@Builder
public class OldSmokeFireHardhatAnalyticsModule {
    int alertDelay;
    OldSmokeFireHardhatAdvancedSettings advancedSettings;

    JsonNode hardwareSettings;
    JsonNode polygons;
}
