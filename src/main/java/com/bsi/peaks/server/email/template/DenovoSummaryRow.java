package com.bsi.peaks.server.email.template;

/**
 * @author span
 * Created by span on 2019-03-11
 */
@SuppressWarnings({"unused"})
public class DenovoSummaryRow {
    private String sampleName;
    private int ms2Count;
    private int psmCount;
    private int psm30Count;
    private int psm50Count;
    private int psm70Count;
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

    public int getPsm30Count() {
        return psm30Count;
    }

    public void setPsm30Count(int psm30Count) {
        this.psm30Count = psm30Count;
    }

    public int getPsm50Count() {
        return psm50Count;
    }

    public void setPsm50Count(int psm50Count) {
        this.psm50Count = psm50Count;
    }

    public int getPsm70Count() {
        return psm70Count;
    }

    public void setPsm70Count(int psm70Count) {
        this.psm70Count = psm70Count;
    }

    public String getNotes() {
        return notes;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }
}
