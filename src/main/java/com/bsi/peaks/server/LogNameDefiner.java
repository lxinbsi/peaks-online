package com.bsi.peaks.server;

import ch.qos.logback.core.PropertyDefinerBase;

import java.util.UUID;

/**
 * @author span
 * Created by span on 2019-04-08
 */
public class LogNameDefiner extends PropertyDefinerBase {
    public static final String INSTANCE_ID_KEY = "instanceId";
    public static final String LOG_FOLDER = "logFolder";
    public static final String INSTANCE_TYPE = "instanceType";
    private String propertyLookupKey;

    public void setPropertyLookupKey(String propertyLookupKey) {
        this.propertyLookupKey = propertyLookupKey;
    }

    @Override
    public String getPropertyValue() {
        switch (propertyLookupKey) {
            case INSTANCE_ID_KEY:
                switch (Launcher.launcherType) {
                    case MASTER:
                        return "master";
                    case WORKER:
                        return "worker";
                    default:
                        return Launcher.taskId;
                }
            case LOG_FOLDER:
            case INSTANCE_TYPE:
                switch (Launcher.launcherType) {
                    case MASTER:
                        return "master";
                    case WORKER:
                        return "worker";
                    default:
                        return "task";
                }
            default:
                throw new IllegalArgumentException("Undefined property lookup key: " + propertyLookupKey);
        }
    }
}
