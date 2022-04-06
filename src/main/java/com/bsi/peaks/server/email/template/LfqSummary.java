package com.bsi.peaks.server.email.template;

public class LfqSummary {
    int featureCount;
    int featureIdCount;
    int featureVectorsIdCount;
    int proteinGroupCount;

    public int getFeatureCount() {
        return featureCount;
    }

    public LfqSummary setFeatureCount(int featureCount) {
        this.featureCount = featureCount;
        return this;
    }

    public int getFeatureIdCount() {
        return featureIdCount;
    }

    public LfqSummary setFeatureIdCount(int featureIdCount) {
        this.featureIdCount = featureIdCount;
        return this;
    }

    public int getFeatureVectorsIdCount() {
        return featureVectorsIdCount;
    }

    public LfqSummary setFeatureVectorsIdCount(int featureVectorsIdCount) {
        this.featureVectorsIdCount = featureVectorsIdCount;
        return this;
    }

    public int getProteinGroupCount() {
        return proteinGroupCount;
    }

    public LfqSummary setProteinGroupCount(int proteinGroupCount) {
        this.proteinGroupCount = proteinGroupCount;
        return this;
    }
}
