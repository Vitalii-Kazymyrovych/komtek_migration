package com.incoresoft.releasesmigrator.dto;

import lombok.AllArgsConstructor;
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
@AllArgsConstructor
public class NewSmokeFireHardhatAdvancedSettings {
    int alertDelay;
    int sensitivity;

    public NewSmokeFireHardhatAdvancedSettings(OldSmokeFireHardhatAdvancedSettings oldSettings, int alertDelay) {
        this.alertDelay = Math.clamp(alertDelay, 0, 300);
        this.sensitivity = Math.clamp((int) oldSettings.getSensitivity(), 1, 10);
    }
}
