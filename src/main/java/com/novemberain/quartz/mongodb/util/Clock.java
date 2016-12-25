package com.novemberain.quartz.mongodb.util;

import java.util.Date;

/**
 * It's responsibility is to provide current time.
 */
public abstract class Clock {

    /**
     * Return current time in millis.
     */
    public abstract long millis();

    /**
     * Return current Date.
     */
    public abstract Date now();

    /**
     * Return Date created from time in milliseconds.
     */
    public abstract Date fromTime(long millis);

    /**
     * Default implementation that returns system time.
     */
    public static final Clock SYSTEM_CLOCK = new Clock() {
        @Override
        public long millis() {
            return System.currentTimeMillis();
        }

        @Override
        public Date now() {
            return new Date();
        }

        @Override
        public Date fromTime(long millis) {
            return new Date(millis);
        }
    };
}
