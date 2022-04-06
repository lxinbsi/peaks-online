package com.bsi.peaks.server.cluster;

import com.bsi.peaks.algorithm.module.preprocess.MSProcessor;
import com.bsi.peaks.common.compression.LZ4Compressor;
import com.bsi.peaks.model.core.ms.*;
import com.bsi.peaks.model.core.ms.fraction.FractionAttributes;
import com.bsi.peaks.model.core.ms.scan.FloatPeaksData;
import com.bsi.peaks.model.core.ms.scan.PeaksData;
import com.bsi.peaks.model.core.ms.scan.PeaksStats;
import com.bsi.peaks.model.proto.AcquisitionMethod;
import com.bsi.peaks.model.proto.TimstofFrame;
import com.bsi.peaks.reader.ScanBasedPostProcessor;
import com.google.protobuf.ByteString;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.bsi.peaks.algorithm.module.preprocess.MSProcessor.keepTopNPeaksLocally;

public class CentroidingProcessor extends ScanBasedPostProcessor {
    protected static final float digitAccu = 0.0005f;
    protected final FractionAttributes fraction;

    CentroidingProcessor(FractionAttributes fraction) {
        this.fraction = fraction;
    }

    @Override
    protected DataLoadingScan process(DataLoadingScan scan) {
        if (scan.centroided()) {
            return scan;
        } else if (fraction.instrument().massAnalyzers().containsKey(scan.msLevel())){
            return centroid(fraction, scan);
        } else {
            return scan;
        }
    }

    static DataLoadingScan centroid(FractionAttributes fraction, DataLoadingScan scan) {
        int resolution = fraction.instrument().massAnalyzers().get(scan.msLevel()).get(0).resolution();
        float[] mzs = scan.peaksMzFloat();
        float[] intensities = scan.peaksIntensityFloat();
        if (mzs.length > 0) {
            MSProcessor.Peak[] peaks = new MSProcessor.Peak[mzs.length];
            for (int i = 0; i < mzs.length; i++) {
                peaks[i] = new MSProcessor.Peak(mzs[i], intensities[i]);
            }
            if (scan.msLevel() == 1) {
                peaks = centroidMS1(fraction, resolution, peaks);
            } else {
                peaks = MSProcessor.centroid(peaks, resolution, digitAccu);
            }
            if (peaks != null) {
                mzs = new float[peaks.length];
                intensities = new float[peaks.length];
                for (int i = 0; i < peaks.length; i++) {
                    mzs[i] = peaks[i].x;
                    intensities[i] = peaks[i].h;
                }
            } else {
                mzs = new float[0];
                intensities = new float[0];
            }
        }
        PeaksData peaksData = new FloatPeaksData(mzs, intensities);
        return new DataLoadingScanBuilder()
            .from(scan)
            .centroided(true)
            .from(PeaksStats.generate(peaksData))
            .from(peaksData)
            .build();
    }

    protected static MSProcessor.Peak[] centroidMS1(FractionAttributes fraction, int resolution, MSProcessor.Peak[] peaks) {
        int centroidPeakCountThreshold = 1;

        MassAnalyzer ms1Analyzer = fraction.instrument().massAnalyzers().get(1).get(0);
        if (ms1Analyzer.type().equals(MassAnalyzerType.FTORBI)) {
            centroidPeakCountThreshold = 0;
        } else if (ms1Analyzer.type().equals(MassAnalyzerType.Quadrupole)) {
            centroidPeakCountThreshold = 2;
        } else if (ms1Analyzer.type().equals(MassAnalyzerType.TripleTOF)) {
            centroidPeakCountThreshold = 2;
        }
        peaks = MSProcessor.removeZeroPeaks(peaks);
        if (peaks.length == 0) {
            return peaks;
        }

        //2. learn a more accurate peak width
        double[] peakWidthABC = MSProcessor.fitPeakWidth(peaks, resolution);
        //3. centroid
        peaks = MSProcessor.centroidMS1(peaks, 0.001f, peakWidthABC, centroidPeakCountThreshold);
        return peaks;
    }

    static List<DataLoadingScan> removeNoise(FractionAttributes fraction, List<DataLoadingScan> scansInFrame) {

        List<DataLoadingScan> processedScans = new ArrayList<>();

        if(scansInFrame.size() > 0) {
            List<MSProcessor.Peak[]> scans = new ArrayList<>();
            List<MSProcessor.Peak> peakList = new ArrayList<>();
            int originalPeakCount = 0;
            for (DataLoadingScan s : scansInFrame) {
                MSProcessor.Peak[] parray = new MSProcessor.Peak[s.peaksCount()];
                float[] peaksMzs = s.peaksMzFloat();
                float[] peaksIntensities = s.peaksIntensityFloat();
                for (int j=0; j<s.peaksCount(); j++) {
                    parray[j] = new MSProcessor.Peak(peaksMzs[j],peaksIntensities[j]);
                }
                originalPeakCount += s.peaksCount();
                parray = MSProcessor.removeZeroPeaks(parray);
                if (parray.length > 0) {
                    peakList.addAll(Arrays.asList(parray));
                }
                scans.add(parray);
            }
            Collections.sort(peakList, (p1, p2) -> {
                return Float.compare(p1.x, p2.x);
            });
            MSProcessor.Peak[] parray = new MSProcessor.Peak[peakList.size()];
            for (int i = 0; i < peakList.size(); i++) {
                parray[i] = peakList.get(i);
            }
            if (parray.length>0) {
                float tolerancPPM = 25f;
                int peakCountthreshold = 4;
                //1. remove 0-height peaks
                parray = MSProcessor.removeZeroPeaks(parray);
                //2. learn a more accurate peak width
                double[] peakWidthABC = MSProcessor.fitPeakWidth(parray, 30000);
                //3. centroid
                parray = MSProcessor.centroidMS1(parray, 0.005f, peakWidthABC, 2);
                parray = keepTopNPeaksLocally(parray, 1, 8);
                int[] consecutiveCount = new int[parray.length];
                List<int[]> scanConsecutiveCount = new ArrayList<>();

                for (MSProcessor.Peak[] scan : scans) {
                    int i=0;
                    int j=0;
                    int[] matchCount = new int[scan.length];
                    scanConsecutiveCount.add(matchCount);
                    boolean jmatched = false;
                    while(i<scan.length && j < parray.length) {
                        float tolerance = parray[j].x*tolerancPPM/1E6f;
                        if (scan[i].x < parray[j].x + tolerance) {
                            if (scan[i].x > parray[j].x - tolerance) {
                                jmatched = true;
                                matchCount[i] = consecutiveCount[j]+1;
                            }
                            i++;
                        } else {
                            if (jmatched) {
                                consecutiveCount[j] += 1;
                            } else {
                                consecutiveCount[j] = 0;
                            }
                            jmatched =false;
                            j++;
                        }
                    }

                }

                Arrays.fill(consecutiveCount, 0);
                for (int m=scans.size()-1; m>=0; m--) {
                    MSProcessor.Peak[] scan = scans.get(m);
                    int i=0;
                    int j=0;
                    int[] matchCount = new int[scan.length];
                    boolean jmatched = false;
                    while(i<scan.length && j < parray.length) {
                        float tolerance = parray[j].x*tolerancPPM/1E6f;
                        if (scan[i].x < parray[j].x + tolerance) {
                            if (scan[i].x > parray[j].x - tolerance) {
                                jmatched = true;
                                matchCount[i] = consecutiveCount[j]+1;
                            }
                            i++;
                        } else {
                            if (jmatched) {
                                consecutiveCount[j] += 1;
                            }else {
                                consecutiveCount[j] = 0;
                            }
                            jmatched =false;
                            j++;
                        }
                    }

                    List<MSProcessor.Peak> tmp = new ArrayList<>();
                    for (int n=0; n<matchCount.length; n++) {
                        if (matchCount[n] >= peakCountthreshold || scanConsecutiveCount.get(m)[n] >= peakCountthreshold ) {
                            tmp.add(scan[n]);
                        }
                    }

                    float[] mz = new float[tmp.size()];
                    float[] h= new float[tmp.size()];
                    for (int n=0; n<mz.length; n++) {
                        mz[n] = tmp.get(n).x;
                        h[n]  = tmp.get(n).h;
                    }


                    PeaksData peaksData = new FloatPeaksData(mz, h);
                    processedScans.add(0,
                            new DataLoadingScanBuilder()
                                    .from(scansInFrame.get(m))
                                    .centroided(false)
                                    .from(PeaksStats.generate(peaksData))
                                    .from(peaksData)
                                    .build());
                }

            }
        }

        return processedScans;
    }

}
