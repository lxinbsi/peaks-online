package com.bsi.peaks.server.cluster;

import com.bsi.peaks.algorithm.module.preprocess.MSProcessor;
import com.bsi.peaks.model.core.AcquisitionMethod;
import com.bsi.peaks.model.core.ms.DataLoadingScan;
import com.bsi.peaks.model.core.ms.DataLoadingScanBuilder;
import com.bsi.peaks.model.core.ms.fraction.FractionAttributes;
import com.bsi.peaks.model.core.ms.scan.FloatPeaksData;
import com.bsi.peaks.model.core.ms.scan.PeaksData;
import com.bsi.peaks.model.core.ms.scan.PeaksStats;

public class WiffCentroidingProcessor extends CentroidingProcessor {
    public WiffCentroidingProcessor(FractionAttributes fraction) {
        super(fraction);
    }

    @Override
    protected DataLoadingScan process(DataLoadingScan scan) {
        if (fraction.acquisitionMethod() == AcquisitionMethod.DDA) {
            // don't do centroiding on DDA wiff data
            return scan;
        }

        int resolution = fraction.instrument().massAnalyzers().get(scan.msLevel()).get(0).resolution();
        float[] mzs = scan.peaksMzFloat();
        float[] intensities = scan.peaksIntensityFloat();

        if (mzs.length > 0) {
            float[][] removedBaseLine = MSProcessor.removeBaseLine(mzs, intensities, 0.05f);
            MSProcessor.Peak[] peaks = new MSProcessor.Peak[removedBaseLine[0].length];
            for (int k = 0; k < removedBaseLine[0].length; k++) {
                peaks[k] = new MSProcessor.Peak(removedBaseLine[0][k], removedBaseLine[1][k]);
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
}
