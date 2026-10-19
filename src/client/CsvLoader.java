package client;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads whole numbers from a CSV file. Accepts comma-separated values,
 * one value per line, or a mix. Non-numeric values (e.g. headers) are
 * collected in {@code skipped} so the GUI can report them.
 */
public final class CsvLoader {

    private CsvLoader() {}

    public static List<Integer> load(File file, List<String> skipped) throws IOException {
        List<Integer> numbers = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
            String line;
            while ((line = reader.readLine()) != null) {
                for (String part : line.split(",")) {
                    String value = part.replace("\uFEFF", "").trim(); // remove Excel BOM
                    if (value.isEmpty()) {
                        continue;
                    }
                    try {
                        numbers.add(Integer.parseInt(value));
                    } catch (NumberFormatException e) {
                        skipped.add(value);
                    }
                }
            }
        }
        return numbers;
    }
}