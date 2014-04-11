package bitronix.tm.utils;

import bitronix.tm.TransactionManagerServices;

import java.util.Properties;

/**
 * @author Ilya Labun
 */
public class ConfigurationUtils {
    private ConfigurationUtils() {
    }

    public static void checkNotStarted() {
        if (TransactionManagerServices.isTransactionManagerRunning())
            throw new IllegalStateException("cannot change the configuration while the transaction manager is running");
    }

    public static String getString(Properties properties, String key, String defaultValue) {
        String value = System.getProperty(key);
        if (value == null) {
            value = properties.getProperty(key);
            if (value == null)
                return defaultValue;
        }
        return evaluate(properties, value);
    }

    public static boolean getBoolean(Properties properties, String key, boolean defaultValue) {
        return Boolean.valueOf(getString(properties, key, "" + defaultValue));
    }

    public static int getInt(Properties properties, String key, int defaultValue) {
        return Integer.parseInt(getString(properties, key, "" + defaultValue));
    }

    private static String evaluate(Properties properties, String value) {
        String result = value;

        int startIndex = value.indexOf('$');
        if (startIndex > -1 && value.length() > startIndex + 1 && value.charAt(startIndex + 1) == '{') {
            int endIndex = value.indexOf('}');
            if (startIndex + 2 == endIndex)
                throw new IllegalArgumentException("property ref cannot refer to an empty name: ${}");
            if (endIndex == -1)
                throw new IllegalArgumentException("unclosed property ref: ${" + value.substring(startIndex + 2));

            String subPropertyKey = value.substring(startIndex + 2, endIndex);
            String subPropertyValue = getString(properties, subPropertyKey, null);

            result = result.substring(0, startIndex) + subPropertyValue + result.substring(endIndex + 1);
            return evaluate(properties, result);
        }

        return result;
    }

}
