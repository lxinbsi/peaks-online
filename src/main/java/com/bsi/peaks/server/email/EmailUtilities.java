package com.bsi.peaks.server.email;

import com.bsi.peaks.event.email.SmtpConfig;
import com.bsi.peaks.event.email.SmtpTlsOption;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.model.system.UserBuilder;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.es.UserManager;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

/**
 * @author span
 * Created by span on 2019-03-07
 */
class EmailUtilities {
    static final SmtpConfig DEFAULT_CONFIG = SmtpConfig.newBuilder()
        .setHostname("")
        .setPort(25)
        .setUsername("")
        .setPassword("")
        .setSsl(false)
        .setTrustAll(false)
        .setStartTls(SmtpTlsOption.SMTP_TLS_DISABLED)
        .setFromAddress("")
        .setCc("")
        .build();

    static final User ADMIN = new UserBuilder()
        .username("admin")
        .userId(UUID.fromString(UserManager.ADMIN_USERID))
        .role(User.Role.SYS_ADMIN)
        .locale("en")
        .build();

    static String generateResultURL(SystemConfig systemConfig, UUID projectId, UUID analysisId) {
        String hostname = systemConfig.getEmailDomainName();
        final String serverRootURL;
        if (hostname.equals("")) {
            serverRootURL = systemConfig.getServerRootURL() + serverPort(systemConfig);
        } else {
            if (hostname.contains(":")) {
                serverRootURL = hostname;
            } else {
                serverRootURL = hostname + serverPort(systemConfig);
            }
        }
        return String.format("%s/project/%s/analysis/%s/result/", serverRootURL, projectId.toString(), analysisId.toString());
    }

    @NotNull
    private static String serverPort(SystemConfig systemConfig) {
        return systemConfig.getServerHttpPort() == 80 ? "" : ":" + systemConfig.getServerHttpPort();
    }

}
