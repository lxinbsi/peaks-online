package com.bsi.peaks.server.service;


import com.bsi.peaks.server.LogNameDefiner;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogService {
    private static final Logger LOG = LoggerFactory.getLogger(LogService.class);
    private static final Pattern LOG_DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}");
    private static final String LOG_FOLDER = "${" + LogNameDefiner.LOG_FOLDER + "}";
    private static final String INSTANCE_ID = "${" + LogNameDefiner.INSTANCE_ID_KEY + "}";
    private static final String HOSTNAME = "${HOSTNAME}";
    private static final String DATE = "%d{yyyy-MM-dd}";
    private static final String LOG_FILE_DIRECTORY = "logs/" + LOG_FOLDER;
    private static final String LOG_FILE_NAME = INSTANCE_ID + "." + HOSTNAME + "." + DATE + ".log";

    public static final DateTimeFormatter LOG_FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final String LOG_FILE_PATH_PATTERN = LOG_FILE_DIRECTORY + "/" + LOG_FILE_NAME;
    public static final String DATE_PATTERN_LOG_LINE = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final DateTimeFormatter LOG_DATE_PATTERN_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN_LOG_LINE)
        .withZone(ZoneId.systemDefault());

    private final String instanceId;
    private final String hostName;
    private final String logFolder;

    public LogService(String hostName) {
        this.hostName = hostName;
        LogNameDefiner logNameDefiner = new LogNameDefiner();
        logNameDefiner.setPropertyLookupKey(LogNameDefiner.INSTANCE_ID_KEY);
        instanceId = logNameDefiner.getPropertyValue();
        logNameDefiner.setPropertyLookupKey(LogNameDefiner.LOG_FOLDER);
        logFolder = logNameDefiner.getPropertyValue();
    }

    public LogService() {
        this(getHostName());
    }

    private static String getHostName() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            return System.getenv("COMPUTERNAME");
        }
        //linux machines
        if (os.contains("nix")) {
            //for Ubuntu we need to run to get the host name for the System.getEnv
            execReadToString("export HOSTNAME");
            String hostname = System.getenv("HOSTNAME");
            if (hostname == null || hostname.isEmpty()) {
                hostname = execReadToString("hostname");
            }
            if (hostname == null || hostname.isEmpty()) {
                hostname = execReadToString("cat /etc/hostname");
            }
            return hostname;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Unable to query hostname for machine. Logs will be unavailable through the UI.", e);
            return null;
        }
    }

    private static String execReadToString(String execCommand) {
        try (Scanner s = new Scanner(Runtime.getRuntime().exec(execCommand).getInputStream()).useDelimiter("\\A")) {
            return s.hasNext() ? s.next().trim() : "";
        } catch (Exception e) {
            LOG.error("Unable to exec and read result for command " + execCommand, e);
            return null;
        }
    }

    private String logFileDir() {
        return LOG_FILE_DIRECTORY
            .replace(LOG_FOLDER, logFolder);
    }

    public String logFileName(Temporal date) {
        return LOG_FILE_PATH_PATTERN
            .replace(LOG_FOLDER, logFolder)
            .replace(INSTANCE_ID, instanceId)
            .replace(HOSTNAME, hostName)
            .replace(DATE, LOG_FILE_DATE_FORMAT.format(date));
    }

    private Path logFilePath(Temporal date) {
        return Paths.get(logFileName(date));
    }

    @NotNull
    private Matcher logDatePatternMatcher(String logLine) {
        return LOG_DATE_PATTERN.matcher(logLine);
    }

    private static <T> void addToList(List<T> list, int index, Iterator<T> it) {
        assert index <= list.size();
        for(int i = index; it.hasNext(); i++) {
            list.add(i, it.next());
        }
    }

    private String[] sortedLogFileNames() {
        File dir = new File(logFileDir());
        String datePatternMatch = "\\d{4}-\\d{2}-\\d{2}";
        final Pattern pattern = Pattern.compile(LOG_FILE_NAME
            .replace(INSTANCE_ID, instanceId)
            .replace(HOSTNAME, hostName)
            .replace(DATE, datePatternMatch));

        String[] list = dir.list((d, name) -> pattern.matcher(name).find());
        final Pattern datePattern = Pattern.compile(datePatternMatch);

        Comparator<String> dateComparator = Comparator.comparing(s -> {
                Matcher matcher = datePattern.matcher(s);
                matcher.find();
                String group = matcher.group(0);
                return LocalDate.parse(group, LOG_FILE_DATE_FORMAT);
            },
            LocalDate::compareTo
        );
        Arrays.sort(list, dateComparator.reversed());

        return list;
    }

    public List<String> log(final int lines) throws IOException {
        long linesToRead = lines;
        final List<String> allLogLines = new ArrayList<>();

        for(String logFileName : sortedLogFileNames()) {
            final Path path = Paths.get(logFileDir() + "/" + logFileName);
            if (linesToRead <= 0) break;
            final long linesInFile;
            try(Stream<String> logLineStream = Files.lines(path)) {
                linesInFile = logLineStream.count();
            }

            final long linesToSkip = Math.max(0, linesInFile - linesToRead);
            final long linesRead = linesInFile - linesToSkip;

            try(Stream<String> logLineStream = Files.lines(path)) {
                addToList(allLogLines, 0, logLineStream
                    .skip(linesToSkip)
                    .limit(linesToRead)
                    .iterator());
            }

            linesToRead -= linesRead;
        }

        return allLogLines;
    }

    public List<String> log(ZonedDateTime start, ZonedDateTime end) throws IOException {
        final Instant startInstant = Instant.from(start);
        final Instant endInstant = Instant.from(end);
        final List<String> allLogLines = new ArrayList<>();

        List<Temporal> sortedDates = Stream.iterate(start, date -> date.plus(1, ChronoUnit.DAYS))
            .limit(ChronoUnit.DAYS.between(start, end.plus(1, ChronoUnit.DAYS)))
            .collect(Collectors.toList());

        for(Temporal date : sortedDates) {
            Path path = logFilePath(date);
            if (!Files.exists(path)) continue;
            try(Stream<String> logLineStream = Files.lines(path)) {
                logLineStream.forEach(l -> allLogLines.add(l));
            }
        }

        List<LogsGroupedByTime> grouped = new ArrayList<>();
        for(String log : allLogLines) {
            Matcher matcher = logDatePatternMatcher(log);
            if (matcher.find()) { //check if log is timed
                final Instant instant = Instant.from(LOG_DATE_PATTERN_FORMATTER.parse(matcher.group(0)));
                grouped.add(new LogsGroupedByTime(instant, log));
            } else if (grouped.size() == 0) {
                //skip we do not know its time
            } else { //log is not timed so use previous log's time
                grouped.get(grouped.size() - 1).add(log);
            }
        }

        return grouped.stream()
            .filter(l -> l.inRange(startInstant, endInstant))
            .flatMap(LogsGroupedByTime::stream)
            .collect(Collectors.toList());
    }

    class LogsGroupedByTime {
        Instant instant;
        List<String> logs;

        private LogsGroupedByTime(Instant instant, String log) {
            logs = new ArrayList<>();
            logs.add(log);
            this.instant = instant;
        }

        private LogsGroupedByTime add(String log) {
            logs.add(log);
            return this;
        }

        private boolean inRange(Instant start, Instant end) {
            return instant.compareTo(start) >= 0 && instant.compareTo(end) <= 0;
        }

        private Stream<String> stream() {
            return logs.stream();
        }
    }
}
