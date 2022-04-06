package com.bsi.peaks.server.email.template;

import java.util.List;
import java.util.UUID;

/**
 * @author span
 * Created by span on 2019-03-08
 * used for hbs template
 */
@SuppressWarnings({"unused"})
public class AnalysisDoneTemplate {
    private UUID userId;
    private UUID projectId;
    private UUID analysisId;
    private String username;
    private String userFullName;
    private String userEmail;
    private String projectName;
    private String analysisName;
    private String resultURL;
    private List<DenovoSummaryRow> denovoSummary;
    private List<IDSummaryRow> dbSummary;
    private List<IDSummaryRow> ptmFinderSummary;
    private List<IDSummaryRow> spiderSummary;
    private SilacSummaryTemplate silacSummary;
    private List<SlSummaryRow> slSummary;
    private LfqSummary lfqSummary;
    private RiqSummary riqSummary;

    public boolean getHasPtmFinder() {
        return ptmFinderSummary != null && ptmFinderSummary.size() > 0;
    }

    public List<IDSummaryRow> getPtmFinderSummary() {
        return ptmFinderSummary;
    }

    public void setPtmFinderSummary(List<IDSummaryRow> ptmFinderSummary) {
        this.ptmFinderSummary = ptmFinderSummary;
    }

    public boolean getHasSpider() {
        return spiderSummary != null && spiderSummary.size() > 0;
    }

    public List<IDSummaryRow> getSpiderSummary() {
        return spiderSummary;
    }

    public void setSpiderSummary(List<IDSummaryRow> spiderSummary) {
        this.spiderSummary = spiderSummary;
    }

    public boolean getHasDb() {
        return dbSummary != null && dbSummary.size() > 0;
    }

    public List<IDSummaryRow> getDbSummary() {
        return dbSummary;
    }

    public void setDbSummary(List<IDSummaryRow> dbSummary) {
        this.dbSummary = dbSummary;
    }

    public boolean getHasDenovo() {
        return denovoSummary != null && denovoSummary.size() > 0;
    }

    public List<DenovoSummaryRow> getDenovoSummary() {
        return denovoSummary;
    }

    public void setDenovoSummary(List<DenovoSummaryRow> denovoSummary) {
        this.denovoSummary = denovoSummary;
    }

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

    public UUID getProjectId() {
        return projectId;
    }

    public void setProjectId(UUID projectId) {
        this.projectId = projectId;
    }

    public UUID getAnalysisId() {
        return analysisId;
    }

    public void setAnalysisId(UUID analysisId) {
        this.analysisId = analysisId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getUserFullName() {
        return userFullName;
    }

    public void setUserFullName(String userFullName) {
        this.userFullName = userFullName;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getAnalysisName() {
        return analysisName;
    }

    public void setAnalysisName(String analysisName) {
        this.analysisName = analysisName;
    }

    public String getResultURL() {
        return resultURL;
    }

    public void setResultURL(String resultURL) {
        this.resultURL = resultURL;
    }

    public SilacSummaryTemplate getSilacSummary() {
        return silacSummary;
    }

    public void setSilacSummary(SilacSummaryTemplate silacSummary) {
        this.silacSummary = silacSummary;
    }

    public boolean getHasSl() {
        return slSummary != null && slSummary.size() > 0;
    }

    public List<SlSummaryRow> getSlSummary() {
        return slSummary;
    }

    public void setSlSummary(List<SlSummaryRow> slSummary) {
        this.slSummary = slSummary;
    }

    public LfqSummary getLfqSummary() {
        return lfqSummary;
    }

    public void setLfqSummary(LfqSummary lfqSummary) {
        this.lfqSummary = lfqSummary;
    }

    public RiqSummary getRiqSummary() {
        return riqSummary;
    }

    public void setRiqSummary(RiqSummary riqSummary) {
        this.riqSummary = riqSummary;
    }
}
