package com.bsi.peaks.server.cluster;

import akka.japi.tuple.Tuple3;
import com.bsi.peaks.common.compression.LZ4Compressor;
import com.bsi.peaks.model.core.AcquisitionMethod;
import com.bsi.peaks.model.core.ms.DataLoadingScan;
import com.bsi.peaks.model.core.ms.DecompressedTimstofFrame;
import com.bsi.peaks.model.core.ms.fraction.FractionAttributes;
import com.bsi.peaks.model.proto.TimstofDIAWindow;
import com.bsi.peaks.model.proto.TimstofFrame;
import com.bsi.peaks.reader.FrameBasedPostProcessor;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class TimstofProcessor extends FrameBasedPostProcessor {
    private final FractionAttributes fraction;

    TimstofProcessor(FractionAttributes fraction) {
        this.fraction = fraction;
    }


    static int findConsecutiveScanCount(float mz, float[] mzs, int[] count, float toleranceInDalton) {
        if (mzs == null || mzs.length==0 || count == null || count.length == 0) {
            return  0;
        }

        int idx = Arrays.binarySearch(mzs, mz);
        if (idx < 0)  idx = - idx -1;
        if (idx >= mzs.length) idx = mzs.length -1 ;

        int maxCount = 0;
        int left = idx-1;
        while(left >=0 && Math.abs(mzs[left]-mz)<toleranceInDalton) {
            if (count[left] + 1 > maxCount) {
                maxCount = count[left] + 1;
            }
            left--;
        }

        int right = idx;
        while (right<mzs.length && Math.abs(mzs[right]-mz)<toleranceInDalton) {
            if (count[right] + 1> maxCount) {
                maxCount = count[right] + 1;
            }
            right++;
        }
        return  maxCount;

    }

    static float[] removeNoise (float[] data, List<Integer> idxToKeep) {
        float[] result = new float[idxToKeep.size()];
        for (int i=0; i<idxToKeep.size(); i++) {
            result[i] = data[idxToKeep.get(i)];
        }
        return result;

    }

    static float[] timsTOFNoiseRemove(float[][] mzs, float[][] intensities, float tolerancPPM) {
        int[][] consecutiveScanCount = new int[mzs.length][];
        int consecutiveCountThreshold = 2;
        consecutiveScanCount[0] = new int[mzs[0].length];

        for (int i=1; i<mzs.length; i++) {
            consecutiveScanCount[i] = new int[mzs[i].length];
            for (int j=0; j<mzs[i].length; j++) {
                float tolerance = tolerancPPM*mzs[i][j]/1e6f;
                int count = findConsecutiveScanCount(mzs[i][j],mzs[i-1],consecutiveScanCount[i-1],tolerance);
                if (count ==0 && i>1) {//allow one gap
                    count = findConsecutiveScanCount(mzs[i][j],mzs[i-2],consecutiveScanCount[i-2],tolerance);
                }
                if (count ==0 && i>2) {//allow one gap
                    count = findConsecutiveScanCount(mzs[i][j],mzs[i-3],consecutiveScanCount[i-3],tolerance);
                }
                consecutiveScanCount[i][j] = count;
            }
        }

        float[][] noisedRemovedmzs = new float[mzs.length][];
        float[][] noisedRemovedIntensities = new float[intensities.length][];


        List<Integer> idxToKeep = new ArrayList<>();
        for (int i=0; i<mzs[mzs.length-1].length; i++) {
            if (consecutiveScanCount[mzs.length-1][i] >= consecutiveCountThreshold) {
                idxToKeep.add(i);
            }
        }

        noisedRemovedmzs[mzs.length-1] = removeNoise(mzs[mzs.length-1],idxToKeep);
        noisedRemovedIntensities[intensities.length-1] = removeNoise(intensities[intensities.length-1],idxToKeep);

        for (int i=mzs.length-2; i>=0; i--) {
            idxToKeep.clear();
            for (int j=0; j<mzs[i].length; j++) {
                float tolerance = tolerancPPM*mzs[i][j]/1e6f;
                int count = findConsecutiveScanCount(mzs[i][j],mzs[i+1],consecutiveScanCount[i+1],tolerance)-1;
                consecutiveScanCount[i][j] = Math.max(count , consecutiveScanCount[i][j]);
                if (count < 0 && i<mzs.length-2) {
                    count = findConsecutiveScanCount(mzs[i][j],mzs[i+2],consecutiveScanCount[i+2],tolerance)-1;
                    consecutiveScanCount[i][j] = Math.max(count , consecutiveScanCount[i][j]);
                }
                if (count < 0 && i<mzs.length-3) {
                    count = findConsecutiveScanCount(mzs[i][j],mzs[i+3],consecutiveScanCount[i+3],tolerance)-1;
                    consecutiveScanCount[i][j] = Math.max(count , consecutiveScanCount[i][j]);
                }
                if (consecutiveScanCount[i][j] >= consecutiveCountThreshold) {
                    idxToKeep.add(j);
                }
            }
            noisedRemovedmzs[i] = removeNoise(mzs[i],idxToKeep);
            noisedRemovedIntensities[i] = removeNoise(intensities[i],idxToKeep);
        }


        float[] tics = new float[mzs.length];
        for (int i=0; i<mzs.length; i++) {
            mzs[i] = noisedRemovedmzs[i];
            intensities[i] = noisedRemovedIntensities[i];
            float tic = 0;
            for (int j=0; j<intensities[i].length; j++) {
                tic += intensities[i][j];
            }
            tics[i] = tic;
        }
        return tics;


    }

    static CompletionStage<TimstofFrame> processFrame(UUID fractionId, TimstofFrame frame) {
        return CompletableFuture.supplyAsync(() -> {
            if (frame.getAcquisitionMethod() == com.bsi.peaks.model.proto.AcquisitionMethod.DIA && frame.getMsLevel() == 2) {
                // only noise remove ms2 DDA frames
                DecompressedTimstofFrame decompressed = DecompressedTimstofFrame.decompress(fractionId, frame);
                float[][] mzs = decompressed.getMzs();
                float[][] intensities = decompressed.getIntensities();
                float[][] noiseRemovedMzs = new float[mzs.length][];
                float[][] noiseRemovedIntensities = new float[mzs.length][];
                int start = 0;
                float[] tics = new float[mzs.length];
                for(TimstofDIAWindow window :frame.getHeader().getDiaWindowsList() ) {
                    float[][] windowMzs = new float[window.getScanEnd()-start][];
                    float[][] windowIntensities = new float[window.getScanEnd()-start][];
                    System.arraycopy(mzs,start,windowMzs,0,windowMzs.length);
                    System.arraycopy(intensities,start,windowIntensities,0,windowIntensities.length);
                    float[] windowTics = timsTOFNoiseRemove(windowMzs, windowIntensities,33);
                    System.arraycopy(windowMzs,0,noiseRemovedMzs,start,windowMzs.length);
                    System.arraycopy(windowIntensities,0,noiseRemovedIntensities,start,windowIntensities.length);
                    System.arraycopy(windowTics,0,tics,start,windowTics.length);
                    start = window.getScanEnd();
                }

                List<Float> ticsList = new ArrayList<>();
                for (float tic : tics) {
                    ticsList.add(tic);
                }


                return frame.toBuilder()
                    .setHeader(frame.getHeader().toBuilder().clearTics().addAllTics(ticsList))
                    .setPeaksMzs(ByteString.copyFrom(LZ4Compressor.compress2DPeaksMzsFormat1(noiseRemovedMzs)))
                    .setPeaksIntensities(ByteString.copyFrom(LZ4Compressor.compress2DPeaksIntensitiesFormat1(noiseRemovedIntensities)))
                    .build();
            } else {
                return frame;
            }
        });
    }


    @Override
    protected List<DataLoadingScan> process(Tuple3<Integer, Integer, List<DataLoadingScan>> tuple3) {
        final int msLevel = tuple3.t1();
        final int frameId = tuple3.t2();
        final List<DataLoadingScan> scansInFrame = tuple3.t3();
        if (scansInFrame.isEmpty()) return scansInFrame;

        // actual processing
        DataLoadingScan firstScan = scansInFrame.get(0);
        if (firstScan.acquisitionMethod() == AcquisitionMethod.DIA) {
            // this is a DIA frame, manipulate the scans
            if(msLevel == 2) {
                return CentroidingProcessor.removeNoise(fraction, scansInFrame);
            } else {
                return scansInFrame;
            }
        } else {
            // this is a DDA frame, manipulate the scans
            // we centroid ms2
            if (msLevel == 1) {
                return scansInFrame;
            } else {
                return scansInFrame.stream()
                    .map(scan -> CentroidingProcessor.centroid(fraction, scan))
                    .collect(Collectors.toList());
            }
        }
    }
}
