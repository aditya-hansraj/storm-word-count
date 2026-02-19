package wordcount.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {
    private final Properties properties;

    public ConfigLoader() {
        this.properties = new Properties();
        loadProperties();
    }

    private void loadProperties() throws RuntimeException{
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find config.properties in resources folder");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new RuntimeException("Error loading configuration file", ex);
        }
    }

    public String getProperty(String key) throws RuntimeException{
        String value = properties.getProperty(key);
        if (value == null) {
            throw new RuntimeException("Missing required configuration key: " + key);
        }
        return value;
    }

    public int getIntProperty(String key) {
        return Integer.parseInt(getProperty(key));
    }
}