package com.incoresoft.releasesmigrator.dto;

import lombok.Builder;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

/**
 * Advanced settings for hardhat and smoke fire for 25.1.
 */
@With
@Value
@Jacksonized
@Builder
public class OldSmokeFireHardhatAdvancedSettings {
    float sensitivity;
}
