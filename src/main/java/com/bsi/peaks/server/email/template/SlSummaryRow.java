package com.bsi.peaks.server.email.template;

public class SlSummaryRow {
    private String sampleName;
    private int ms1Count;
    private int ms2Count;
    private int psmCount;
    private int peptideCount;
    private int sequenceCount;
    private int proteinCount;
    private int proteinGroupCount;
    private int msRun;
    private String psmCountMs2Count;

    public String getSampleName() {
        return sampleName;
    }

    public SlSummaryRow setSampleName(String sampleName) {
        this.sampleName = sampleName;
        return this;
    }

    public int getMs1Count() {
        return ms1Count;
    }

    public SlSummaryRow setMs1Count(int ms1Count) {
        this.ms1Count = ms1Count;
        return this;
    }

    public int getMs2Count() {
        return ms2Count;
    }

    public SlSummaryRow setMs2Count(int ms2Count) {
        this.ms2Count = ms2Count;
        return this;
    }

    public int getPsmCount() {
        return psmCount;
    }

    public SlSummaryRow setPsmCount(int psmCount) {
        this.psmCount = psmCount;
        return this;
    }

    public int getPeptideCount() {
        return peptideCount;
    }

    public SlSummaryRow setPeptideCount(int peptideCount) {
        this.peptideCount = peptideCount;
        return this;
    }

    public int getSequenceCount() {
        return sequenceCount;
    }

    public SlSummaryRow setSequenceCount(int sequenceCount) {
        this.sequenceCount = sequenceCount;
        return this;
    }

    public int getProteinCount() {
        return proteinCount;
    }

    public SlSummaryRow setProteinCount(int proteinCount) {
        this.proteinCount = proteinCount;
        return this;
    }

    public int getProteinGroupCount() {
        return proteinGroupCount;
    }

    public SlSummaryRow setProteinGroupCount(int proteinGroupCount) {
        this.proteinGroupCount = proteinGroupCount;
        return this;
    }

    public int getMsRun() {
        return msRun;
    }

    public SlSummaryRow setMsRun(int msRun) {
        this.msRun = msRun;
        return this;
    }

    public String getPsmCountMs2Count() {
        return psmCountMs2Count;
    }

    public SlSummaryRow setPsmCountMs2Count(String psmCountMs2Count) {
        this.psmCountMs2Count = psmCountMs2Count;
        return this;
    }
}
