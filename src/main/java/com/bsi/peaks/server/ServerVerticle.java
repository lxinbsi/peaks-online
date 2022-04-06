package com.bsi.peaks.server;

import akka.pattern.AskTimeoutException;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.auth.JWTTool;
import com.bsi.peaks.server.auth.VertxUser;
import com.bsi.peaks.server.config.SystemConfig;
import com.bsi.peaks.server.handlers.*;
import com.bsi.peaks.server.handlers.helper.VertxChunkedOutputStream;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.auth.AuthProvider;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.UserSessionHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * A verticle for main server
 *
 * @author Shengying Pan
 *         Created by span on 1/5/17.
 */
public class ServerVerticle extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(ServerVerticle.class);
    private static final String ASK_TIMEOUT_ERROR_MESSAGE = "Server is busy. Please try again after a few minutes";

    private SystemConfig config;
    private Injector injector;

    @Inject
    public ServerVerticle(final Vertx vertx, final Injector injector, final SystemConfig config) {
        this.vertx = vertx;
        this.injector = injector;
        this.config = config;
    }

    @Override
    public void start(final Future<Void> started) throws Exception{
        // don't automatically clean uploaded files, so we can resume
        cleanUploadedFiles();
        Router router = Router.router(vertx);

        String root = config.getApiRoot();
        if (config.getApiAllowCors()) {
            // allow cross domain access
            Set<HttpMethod> allowedMethods = new HashSet<>(Arrays.asList(
                HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE
            ));

            Set<String> allowedHeaders = new HashSet<>(Arrays.asList(
                "accept", "authorization", "content-type"
            ));

            router.route(root + "/*").handler(CorsHandler.create(".*")
                .allowedHeaders(allowedHeaders)
                .allowedMethods(allowedMethods)
                .allowCredentials(true)
            );
        }

        // auth
        router.route().handler(CookieHandler.create());
        router.route().handler(
            SessionHandler.create(LocalSessionStore.create(vertx)).setSessionTimeout(config.getServerSessionTimeout().toMillis())
        );
        AuthProvider authProvider = injector.getInstance(AuthProvider.class);
        router.route().handler(UserSessionHandler.create(authProvider));

        JWTTool jwtTool = injector.getInstance(JWTTool.class);
        router.route().handler(context -> {
            if (context.user() == null) {
                String jwt = context.request().getHeader("authorization");
                User user = jwtTool.decode(jwt);
                if (user != null) {
                    context.setUser(new VertxUser(user, jwt, authProvider));
                }
            }
            context.next();
        });

        // attach no caching headers to all api routes
        router.route(root + "/*").handler(context -> {
            context.response().putHeader("Cache-Control", "no-cache, no-store, must-revalidate");
            context.response().putHeader("Pragma", "no-cache");
            context.response().putHeader("Expires", "0");
            context.next();
        });

        // bind api routes
        router.mountSubRouter(root + "/users", injector.getInstance(UserHandler.class).createSubRouter());
        router.mountSubRouter(root + "/projects", injector.getInstance(ProjectHandler.class).createSubRouter());
        router.mountSubRouter(root + "/exporting", injector.getInstance(ExportHandler.class).createSubRouter());
        router.mountSubRouter(root + "/cluster", injector.getInstance(ClusterStateHandler.class).createSubRouter());
        router.mountSubRouter(root + "/enzymes", injector.getInstance(EnzymeHandler.class).createSubRouter());
        router.mountSubRouter(root + "/instruments", injector.getInstance(InstrumentHandler.class).createSubRouter());
        router.mountSubRouter(root + "/modifications", injector.getInstance(ModificationHandler.class).createSubRouter());
        router.mountSubRouter(root + "/databases", injector.getInstance(DatabaseHandler.class).createSubRouter());
        router.mountSubRouter(root + "/query", injector.getInstance(AnalysisProtoHandler.class).createSubRouter());
        router.mountSubRouter(root + "/sample/query", injector.getInstance(SampleProtoHandler.class).createSubRouter());
        router.mountSubRouter(root + "/constant", injector.getInstance(ConstantHandler.class).createSubRouter());
        router.mountSubRouter(root + "/events", injector.getInstance(EventsHandler.class).createSubRouter());
        router.mountSubRouter(root + "/state", injector.getInstance(StateHandler.class).createSubRouter());
        router.mountSubRouter(root + "/remote-upload", injector.getInstance(RemoteUploadHandler.class).createSubRouter());
        router.mountSubRouter(root + "/monitor", injector.getInstance(MonitorHandler.class).createSubRouter());
        router.mountSubRouter(root + "/email", injector.getInstance(EmailHandler.class).createSubRouter());
        router.mountSubRouter(root + "/task", injector.getInstance(TaskHandler.class).createSubRouter());
        router.mountSubRouter(root + "/zip", injector.getInstance(ZipExportHandler.class).createSubRouter());
        router.mountSubRouter(root + "/reporterionqmethod", injector.getInstance(ReportIonQMethodHandler.class).createSubRouter());
        router.mountSubRouter(root + "/silacqmethod", injector.getInstance(SilacQMethodHandler.class).createSubRouter());
        router.mountSubRouter(root + "/workflow", injector.getInstance(WorkflowHandler.class).createSubRouter());
        router.mountSubRouter(root + "/archive", injector.getInstance(ArchivingHandler.class).createSubRouter());
        router.mountSubRouter(root + "/spectrallibrary", injector.getInstance(SpectralLibraryHandler.class).createSubRouter());
        router.mountSubRouter(root + "/logo", injector.getInstance(CustomLogoTextHandler.class).createSubRouter());
        router.mountSubRouter(root + "/instrumentdaemon", injector.getInstance(InstrumentDaemonHandler.class).createSubRouter());
        router.mountSubRouter("/swagger", injector.getInstance(SwaggerHandler.class).createSubRouter());

        // failure handling
        failureHandler(router.route(root + "/*"));

        // if in integration mode, serve web content from Vertx directly
        if (config.isServerWebIntegration()) {
            LOG.info("Serving web content from Vertx");
            router.get().pathRegex("^(.*\\.js|.*\\.css|.*\\.png|.*\\.jpg|.*\\.jpeg|.*\\.gif|.*\\.ttf|.*\\.woff2)$.*").handler(context -> {
                context.response().putHeader("Cache-Control", "max-age=604800");
                context.next();
            });

            router.get("/index.html").handler(context -> {
                context.response().putHeader("Cache-Control", "no-cache, no-store, must-revalidate"); // HTTP 1.1.
                context.response().putHeader("Pragma", "no-cache"); // HTTP 1.0.
                context.response().putHeader("Expires", "-1"); // Proxies.
                context.next();
            });

            StaticHandler webHandler = StaticHandler.create()
                .setWebRoot("web")
                .setIndexPage("index.html")
                .setCachingEnabled(false)
                .setDefaultContentEncoding("UTF-8")
                .setFilesReadOnly(true);
            router.route("/*").handler(webHandler);


            router.get().pathRegex("^(?!(" + root.replace("/", "\\/") + ".*)$).*").handler(context -> {
                // redirect anything that is not an api call to the index page
                context.reroute("/index.html");
            });

            router.route("/download/cli.zip").handler(context -> {
                HttpServerResponse response = context.response();
                response.putHeader("Content-Disposition", "attachment; filename=\"cli.zip\"");
                response.putHeader("Content-Type", "application/zip, application/octet-stream");
                //Do not cache CLI download
                response.putHeader("Cache-Control", "no-cache, no-store, must-revalidate"); // HTTP 1.1.
                response.putHeader("Pragma", "no-cache"); // HTTP 1.0.
                response.putHeader("Expires", "-1"); // Proxies.
                response.setChunked(true);
                OutputStream out = new VertxChunkedOutputStream(response);
                try (InputStream in = getClass().getClassLoader().getResourceAsStream("web/download/cli.zip")) {
                    ByteStreams.copy(in, out);
                } catch (Exception e) {
                    LOG.error("Unable to download cli from server", e);
                    response.setStatusCode(500).setStatusMessage("Unable to provide cli").end();
                    return;
                }
                response.end();
            });
        }
        HttpServerOptions serverOptions = new HttpServerOptions();

        final String certificateLocation = config.getCertificateLocation();
        final String certificatePassword = config.getCertificatePassword();
        if (!certificateLocation.equals("") && !certificatePassword.equals("")) {
            try {
                KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
                keystore.load(new FileInputStream(certificateLocation), certificatePassword.toCharArray());
                Enumeration aliases = keystore.aliases();
                for(; aliases.hasMoreElements();) {
                    String alias = (String) aliases.nextElement();
                    Date certExpiryDate = ((X509Certificate) keystore.getCertificate(alias)).getNotAfter();
                    SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");
                    Date today = new Date();
                    long dateDiff = certExpiryDate.getTime() - today.getTime();
                    long expiresIn = dateDiff / (24 * 60 * 60 * 1000);
                    System.out.println("Certifiate: " + alias + "\tExpires On: " + certExpiryDate + "\tFormated Date: " +
                        ft.format(certExpiryDate) + "\tToday's Date: " + ft.format(today) + "\tExpires In: "+ expiresIn +
                        " days");
                }
            } catch (Exception e) {
                if (e instanceof FileNotFoundException) {
                    LOG.error("No ssl certificate found at specified location: " + certificateLocation);
                }
                if (e instanceof IOException) {
                    LOG.error("Incorrect ssl certificate password");
                }
                throw e;
            }

            serverOptions.setSsl(true)
                .setKeyCertOptions(new JksOptions().
                    setPath(certificateLocation).
                    setPassword(certificatePassword));
        }

        serverOptions.setCompressionSupported(true);
        int port = config.getServerHttpPort();
        vertx.createHttpServer(serverOptions)
            .requestHandler(router::accept)
            .listen(port, result -> {
                if (result.succeeded()) {
                    started.complete();
                    LOG.info("Server started successfully at port " + port);
                } else {
                    started.fail(result.cause());
                    LOG.info("Server failed to start", result.cause());
                }
            });
    }

    public static void failureHandler(Route rootRoute) {
        ExceptionExtractor exceptionExtractor = new ExceptionExtractor();

        rootRoute.failureHandler(context -> {
            final HttpServerRequest request = context.request();
            final String req;
            if (request == null) {
                req = "NULL request";
            } else {
                req = request.method() + " " + request.path();
            }
            Throwable error = context.failure();
            if (error == null) {
                LOG.error(req + " : Route failure NULL");
                context.response().setStatusCode(context.statusCode()).end();
                return;
            }
            for (Throwable throwable : Lists.reverse(Throwables.getCausalChain(error))) {
                if (throwable instanceof HandlerException) {
                    HandlerException he = (HandlerException) throwable;
                    final int errorCode = he.getErrorCode();
                    if (errorCode >= 400 && errorCode < 500) {
                        LOG.debug(req +" : " + he.getMessage(), he.getCause());
                    } else {
                        LOG.error(req +" : " + he.getMessage(), he.getCause());
                    }
                    String errorMessage = exceptionExtractor.beautifyException(he);
                    context.response().setStatusCode(errorCode).setStatusMessage(errorMessage).end();
                    return;
                } else if (throwable instanceof AskTimeoutException) {
                    context.response().setStatusCode(503).setStatusMessage(ASK_TIMEOUT_ERROR_MESSAGE).end();
                    return;
                } else {
                    String errorMessage = exceptionExtractor.extractException(throwable);
                    if (!errorMessage.isEmpty()) {
                        context.response().setStatusCode(400).setStatusMessage(errorMessage).end();
                        return;
                    }
                }
            }

            LOG.error(req, error);
            if (error instanceof VertxException && "Connection was closed".equals(error.getMessage())) {
                LOG.debug("No 500 response sent, since connection is already closed.");
            } else {
                String errorMessage = exceptionExtractor.beautifyException(error);
                context.response().setStatusCode(500).setStatusMessage(errorMessage).end();
            }
        });
    }

    private boolean isEmptyDir(File dir) {
        if (dir.exists() && dir.isDirectory()) {
            File[] children = dir.listFiles();
            if (children != null) {
                if (children.length == 0) {
                    return true;
                } else if (children.length == 1) {
                    return isEmptyDir(children[0]);
                } else {
                    return false;
                }
            } else {
                return true;
            }
        } else {
            return false;
        }
    }

    private void cleanUploadedFiles() {
        File uploadDir = new File(ProjectHandler.UPLOADS_DIR);
        if (uploadDir.exists() && uploadDir.isDirectory() && uploadDir.listFiles() != null) {
            for (File file : uploadDir.listFiles()) {
                // now we only clean empty folders
                try {
                    if (isEmptyDir(file)) {
                        FileUtils.deleteDirectory(file);
                    } /*else {
                        if (!file.delete()) {
                            throw new IOException("Unable to delete file " + file.getAbsolutePath());
                        }
                    }*/
                } catch (IOException e) {
                    // log it to debug level as its not very important whether the file is deleted.
                    LOG.debug("Unable to delete empty folder {}", file.getAbsolutePath());
                }
            }
        }
    }
}
