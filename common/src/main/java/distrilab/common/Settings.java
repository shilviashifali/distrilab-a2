package distrilab.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Runtime settings, so nothing is hard-coded in the programs.
 * Each value is looked up in this order:
 *   1. command-line argument  --key=value
 *   2. distrilab.properties in the current folder
 *   3. the default given by the caller
 */
public final class Settings {

    public static final String FILE_NAME = "distrilab.properties";

    private final Properties values = new Properties();

    private Settings() {}

    public static Settings load(String[] args) {
        Settings s = new Settings();
        Path file = Path.of(FILE_NAME);
        if (Files.exists(file)) {
            try (InputStream in = Files.newInputStream(file)) {
                s.values.load(in);
            } catch (IOException e) {
                Log.error("Settings", "Could not read " + FILE_NAME + ": " + e.getMessage());
            }
        }
        for (String arg : args) {
            if (arg.startsWith("--") && arg.contains("=")) {
                int eq = arg.indexOf('=');
                s.values.setProperty(arg.substring(2, eq), arg.substring(eq + 1));
            } else {
                Log.error("Settings", "Ignoring argument '" + arg + "' (use --key=value)");
            }
        }
        return s;
    }

    public String get(String key, String defaultValue) {
        return values.getProperty(key, defaultValue).trim();
    }

    public int getInt(String key, int defaultValue) {
        String v = values.getProperty(key);
        if (v == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Setting '" + key + "' must be a whole number, got '" + v + "'");
        }
    }
}
