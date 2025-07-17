package org.apache.dolphinscheduler.common.utils;

public final class ServerHostKeyUtils {
    public static String getAddress(String hostKey) {
        if (hostKey != null) {
            return hostKey.split("#")[0];
        }
        return null;
    }

    public static long getStartupTime(String hostKey) {
        if (hostKey != null) {
            final String[] partials = hostKey.split("#");
            if (partials.length == 2) {
                return new Long(partials[1]);
            }
        }
        return 0;
    }

    public static String toHostKey(String address, long startupTime) {
        return String.format("%s#%d", address, startupTime);
    }
}
