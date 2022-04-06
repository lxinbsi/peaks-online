package com.bsi.peaks.server.handlers.helper;

import akka.Done;
import com.bsi.peaks.model.dto.FilePath;
import com.bsi.peaks.model.dto.FileStructureNode;
import com.bsi.peaks.model.dto.InstrumentDaemonSampleInfo;
import com.bsi.peaks.model.dto.ParsingRule;
import com.bsi.peaks.model.dto.Sample;
import com.bsi.peaks.model.dto.SampleSubmission;
import com.bsi.peaks.utilities.FTPHelper;
import com.bsi.peaks.utilities.LFSHelper;
import com.google.common.collect.Lists;
import io.vertx.core.file.FileSystem;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class InstrumentDaemonHelper {
    public static SampleSubmission createSamplesFromInstrumentDaemonSampleInfo(
        InstrumentDaemonSampleInfo sampleInfo
    ) {
        SampleSubmission.Builder sampleSubmissionBuilder = SampleSubmission.newBuilder();
        ParsingRule parsingRule = sampleInfo.getParsingRule();
        int samplesInProject = parsingRule.getSamplesInProject();
        int fractionsPerSample = parsingRule.getFractionsPerSample();

        List<String> delimiters = getDelimiters(parsingRule);

        List<String> parsedList = matchesParsingRule(parsingRule.getExample(), delimiters, parsingRule);
        String projectName = parsedList.get(0);
        String sampleName = parsedList.get(1);

        sampleInfo.getInstrumentDaemonSamplesList().forEach(daemonSample -> {
            String instrumentType = daemonSample.getInstrumentType();
            for (int i = 0 ; i < samplesInProject; i += 1) {
                Sample.Builder sampleBuilder = Sample.newBuilder();
                StringBuilder sampleNameBuilder = new StringBuilder(projectName);
                sampleNameBuilder.append("_");
                sampleNameBuilder.append(instrumentType);
                sampleNameBuilder.append("_");
                sampleNameBuilder.append(sampleNameBuilder);
                sampleNameBuilder.append(i + 1);
                sampleBuilder.setName("sample" + i);
                for (int j = 0; j < fractionsPerSample; j += 1) {
                    if (j == 0) {
                        sampleBuilder.setName(sampleNameBuilder.toString())
                            .setInstrument(instrumentType)
                            .setEnzyme(daemonSample.getEnzyme())
                            .setActivationMethod(daemonSample.getActivationMethod())
                            .setAcquisitionMethod(daemonSample.getAcquisitionMethod())
                            .build();
                    }
                    Sample.Fraction fraction = Sample.Fraction.newBuilder()
                        .setSourceFile("fraction" + (j + 1))
                        .build();
                    sampleBuilder.addFractions(fraction);
                }
                sampleSubmissionBuilder.addSample(sampleBuilder.build());
            }
        });
        return sampleSubmissionBuilder.build();
    }

    public static List<String> getDelimiters(ParsingRule parsingRule) {
        List<String> delimiters = new ArrayList<>();
        for (int i = 0; i < parsingRule.getDelimiterSelectionsList().size(); i++) {
            if (parsingRule.getDelimiterSelectionsList().get(i)) {
                String delimiter = parsingRule.getDelimitersList().get(i);
                if (delimiter.equals(".")) {
                    delimiters.add("\\.");
                } else {
                    delimiters.add(delimiter);
                }
            }
        }
        return delimiters;
    }

    public static List<String> matchesParsingRule(String file, List<String> delimiters, ParsingRule parsingRule) {
        List<String> parsedFileName = new ArrayList<>();
        for (String s : delimiters) {
            if (parsedFileName.size() == 0) {
                parsedFileName = Arrays.asList(file.split(s));
            } else {
                List<String> newList = new ArrayList<>();
                for (String parsedString : parsedFileName) {
                    newList.addAll(Arrays.asList(parsedString.split(s)));
                }
                parsedFileName = newList;
            }
        }

        if (parsedFileName.size() < getMaxPosFromParsingRule(parsingRule)) {
            return Collections.emptyList();
        }

        String projectName =
            getNameFromParsedFileName(parsingRule.getProjectPositionsList(), parsedFileName, new StringBuilder());
        if (projectName.equals("")) return Collections.emptyList();
        String sampleName =
            getNameFromParsedFileName(parsingRule.getSamplePositionsList(), parsedFileName, new StringBuilder());
        if (sampleName.equals("")) return Collections.emptyList();
        String enzyme =
            getNameFromParsedFileName(parsingRule.getEnzymePositionsList(), parsedFileName, new StringBuilder());

        String activationMethod = parsedFileName.get(parsingRule.getActivationPosition());
        return Lists.newArrayList(projectName, sampleName, enzyme, activationMethod);
    }

    public static String getNameFromParsedFileName(
        List<Integer> positionList,
        List<String> parsedFileName,
        StringBuilder builder
    ) {
        boolean first = true;
        for(int pos : positionList) {
            if (pos < parsedFileName.size()) {
                if (!first) {
                    builder.append("_");
                }
                first = false;
                builder.append(parsedFileName.get(pos));
            }
        }
        return builder.toString();
    }

    public static CompletableFuture<Done> writeToDataRepo(
        FilePath filePath,
        File directory,
        String fileName,
        UUID fractionId,
        UUID projectId,
        Logger log
    ) {
        String subPath = filePath.getSubPath();
        String remotePathName = filePath.getRemotePathName();
        String destination = subPath + "/" + fileName;
        for (File file: directory.listFiles()) {
            if (file.getName().equals(fileName)) {
                String absolutePath = file.getAbsolutePath();
                OutputStream finalOutputStream = getRemoteOutputStream(remotePathName, subPath, log, destination);;
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        Files.copy(Paths.get(absolutePath), finalOutputStream);
                        return Done.getInstance();
                    } catch (Throwable e) {
                        log.error("Error copying file {}: {} for project {}", fileName, fractionId, projectId);
                        throw new RuntimeException("Unable to copying file " + fileName + ", fraction id " + fractionId);
                    }
                });
            }
        }
        return CompletableFuture.completedFuture(Done.getInstance());
    }

    public static void writeDirectoryToDataRepo(
        String remotePathName,
        String subPath,
        File sourceFile,
        FileStructureNode fileStructureNode,
        UUID fractionId,
        UUID projectId,
        Logger log
    ) {
        String fileName = fileStructureNode.getName();
        String destination = subPath + "/" + fileName;
        if (!fileStructureNode.getIsDirectory()) {
            try {
                Files.copy(sourceFile.toPath(), getRemoteOutputStream(remotePathName, subPath, log, destination));
            } catch (Throwable e) {
                log.error("Error copying file {}: {} for project {}", fileName, fractionId, projectId);
                throw new RuntimeException("Unable to copying file " + fileName + ", fraction id " + fractionId);
            }
        } else {
            if (LFSHelper.dirExists(remotePathName, subPath)) {
                LFSHelper.mkdir(remotePathName, destination);
            } else if (FTPHelper.dirExists(remotePathName, subPath)) {
                FTPHelper.mkdir(remotePathName, destination);
            } else {
                throw new IllegalStateException("Data repo is not LFS nor FTP");
            }
            for(FileStructureNode node: fileStructureNode.getChildrenList()) {
                for (File file: sourceFile.listFiles()) {
                    if (file.getName().equals(node.getName())) {
                        writeDirectoryToDataRepo(remotePathName, destination, file, node, fractionId, projectId, log);
                    }
                }
            }
        }
    }

    private static OutputStream getRemoteOutputStream(
        String remotePathName,
        String subPath,
        Logger log,
        String destination
    ) {
        if (LFSHelper.dirExists(remotePathName, subPath)) {
            try {
                return LFSHelper.createFile(remotePathName, destination);
            } catch (Exception e) {
                log.error("LFSHelper cannot create file at " + remotePathName + destination);
                e.printStackTrace();
            }
        } else if (FTPHelper.dirExists(remotePathName, subPath)) {
            return FTPHelper.createFile(remotePathName, destination);
        } else {
            throw new IllegalStateException("Data repo is not LFS nor FTP");
        }
        return null;
    }

    public static int countFilesForDaemonTimstof(FileStructureNode fileStructureNode) {
        int count = 0;
        if (fileStructureNode.getIsDirectory()){
            for (FileStructureNode child: fileStructureNode.getChildrenList()) {
                count += countFilesForDaemonTimstof(child);
            }
        } else {
            count = 1;
        }
        return count;
    }

    public static void createFoldersInstrumentDaemon(
        FileStructureNode fileStructureNode,
        FileSystem fileSystem,
        String absolutePath,
        Map<String, String> nameToAbsolutePath
    ) {
        if (fileStructureNode.getIsDirectory()) {
            String curDirPath = absolutePath + '/' + fileStructureNode.getName();
            new File(curDirPath, UUID.randomUUID().toString());
            fileSystem.mkdirsBlocking(curDirPath);
            for (FileStructureNode child : fileStructureNode.getChildrenList()) {
                createFoldersInstrumentDaemon(child, fileSystem, curDirPath, nameToAbsolutePath);
            }
        } else {
            nameToAbsolutePath.put(fileStructureNode.getName(), absolutePath);
        }
    }

    public static int getMaxPosFromParsingRule(ParsingRule parsingRule) {
        final int enzymePositionMax = parsingRule.getEnzymePositionsList().isEmpty() ? 0 :
            Collections.max(parsingRule.getEnzymePositionsList());
        final int projectPositionMax = parsingRule.getProjectPositionsList().isEmpty() ? 0 :
            Collections.max(parsingRule.getProjectPositionsList());
        final int samplePositionMax = parsingRule.getSamplePositionsList().isEmpty() ? 0 :
            Collections.max(parsingRule.getSamplePositionsList());
        final int activationPosition = parsingRule.getActivationPosition();
        return Math.max(Math.max(Math.max(enzymePositionMax, projectPositionMax), samplePositionMax), activationPosition);
    }
}
