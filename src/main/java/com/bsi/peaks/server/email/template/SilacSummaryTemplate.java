package com.bsi.peaks.server.email.template;

public class SilacSummaryTemplate {

    private int featureVectorGroupsCount;
    private int featureVectorsCount;
    private int featuresCount;
    private int peptideSequencesCount;

    public int getFeatureVectorGroupsCount() {
        return featureVectorGroupsCount;
    }

    public void setFeatureVectorGroupsCount(int featureVectorGroupsCount) {
        this.featureVectorGroupsCount = featureVectorGroupsCount;
    }

    public int getFeatureVectorsCount() {
        return featureVectorsCount;
    }

    public void setFeatureVectorsCount(int featureVectorsCount) {
        this.featureVectorsCount = featureVectorsCount;
    }

    public int getFeaturesCount() {
        return featuresCount;
    }

    public void setFeaturesCount(int featuresCount) {
        this.featuresCount = featuresCount;
    }

    public int getPeptideSequencesCount() {
        return peptideSequencesCount;
    }

    public void setPeptideSequencesCount(int peptideSequencesCount) {
        this.peptideSequencesCount = peptideSequencesCount;
    }
}
