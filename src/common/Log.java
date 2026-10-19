package common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public final class Log {
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private Log() {}

    public static void info(String source, String msg) {
        System.out.println("[" + LocalTime.now().format(FMT) + "][" + source + "] " + msg);
    }

    public static void error(String source, String msg) {
        System.err.println("[" + LocalTime.now().format(FMT) + "][" + source + "] ERROR: " + msg);
    }
}