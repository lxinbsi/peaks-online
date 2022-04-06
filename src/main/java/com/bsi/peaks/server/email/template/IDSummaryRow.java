package com.bsi.peaks.server.email.template;

/**
 * @author span
 * Created by span on 2019-03-11
 */
public class IDSummaryRow {
    private String sampleName;
    private int ms2Count;
    private int psmCount;
    private int peptideCount;
    private int sequenceCount;
    private int proteinCount;
    private int proteinGroupCount;
    private String notes;

    public String getSampleName() {
        return sampleName;
    }

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    public int getMs2Count() {
        return ms2Count;
    }

    public void setMs2Count(int ms2Count) {
        this.ms2Count = ms2Count;
    }

    public int getPsmCount() {
        return psmCount;
    }

    public void setPsmCount(int psmCount) {
        this.psmCount = psmCount;
    }

    public int getPeptideCount() {
        return peptideCount;
    }

    public void setPeptideCount(int peptideCount) {
        this.peptideCount = peptideCount;
    }

    public int getSequenceCount() {
        return sequenceCount;
    }

    public void setSequenceCount(int sequenceCount) {
        this.sequenceCount = sequenceCount;
    }

    public int getProteinCount() {
        return proteinCount;
    }

    public void setProteinCount(int proteinCount) {
        this.proteinCount = proteinCount;
    }

    public int getProteinGroupCount() {
        return proteinGroupCount;
    }

    public void setProteinGroupCount(int proteinGroupCount) {
        this.proteinGroupCount = proteinGroupCount;
    }

    public String getNotes() {
        return notes;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }
}
