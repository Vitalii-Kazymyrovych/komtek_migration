package com.incoresoft.releasesmigrator.dto;

import lombok.Builder;
import lombok.Value;
import lombok.With;
import lombok.extern.jackson.Jacksonized;

import java.util.List;

/**
 * Copied from middleware-analytics-lib: {@code com.incoresoft.middleware.analytics.module.alpr.LprAdvancedSettings}.
 */
@With
@Value
@Jacksonized
@Builder
public class LprAdvancedSettings {
    int minPlateWidth;
    int minPlateHeight;
    int framesToDetect;
    String ocrModel;
    int minPlateLength;
    int maxPlateLength;
    boolean templateMatching;
    List<String> countries;
    Float sensitivity;
    String saveFrame;
}
