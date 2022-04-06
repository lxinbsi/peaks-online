package com.bsi.peaks.server.email.template;

public class RiqSummary {
    int psmCount;
    int peptideSequenceCount;
    int proteinGroupCount;

    public int getPsmCount() {
        return psmCount;
    }

    public RiqSummary setPsmCount(int psmCount) {
        this.psmCount = psmCount;
        return this;
    }

    public int getPeptideSequenceCount() {
        return peptideSequenceCount;
    }

    public RiqSummary setPeptideSequenceCount(int peptideSequenceCount) {
        this.peptideSequenceCount = peptideSequenceCount;
        return this;
    }

    public int getProteinGroupCount() {
        return proteinGroupCount;
    }

    public RiqSummary setProteinGroupCount(int proteinGroupCount) {
        this.proteinGroupCount = proteinGroupCount;
        return this;
    }
}
