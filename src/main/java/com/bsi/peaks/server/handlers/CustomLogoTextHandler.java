package com.bsi.peaks.server.handlers;

import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.models.CustomText;
import com.bsi.peaks.server.models.CustomTextBuilder;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Set;

import static java.util.Collections.singletonList;

public class CustomLogoTextHandler extends ApiHandler {
    private static final Logger LOG = LoggerFactory.getLogger(CustomLogoTextHandler.class);
    final String logoFileName = "logo.png";
    final String customTextFileName = "customText";

    @Inject
    public CustomLogoTextHandler(Vertx vertx) {
        super(vertx);
    }

    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.post("/").handler(BodyHandler.create()).handler(this::postCustomLogoAndText);
        subRouter.post("/reset").handler(this::resetCustomLogoAndText);
        subRouter.get("/").handler(this::getCustomTextOnly);
        subRouter.get("/image").handler(this::getLogoOnly);
        return subRouter;
    }

    private void postCustomLogoAndText(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final HttpServerResponse response = context.response();
            final HttpServerRequest request = context.request();

            // extract the texts
            final String headMessage = request.getFormAttribute("headMessage");
            final String contentMessage = request.getFormAttribute("contentMessage");
            final String website = request.getFormAttribute("website");
            final CustomText customText =
                new CustomTextBuilder()
                .headMessage(headMessage == null ? "" : headMessage)
                .contentMessage(contentMessage == null ? "" : contentMessage)
                .website(website == null ? "" : website).build();

            // save custom text as immutable and write to file
            try {
                FileOutputStream f = new FileOutputStream(new File(customTextFileName));
                ObjectOutputStream o = new ObjectOutputStream(f);

                o.writeObject(customText);

                o.close();
                f.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
            // transfer logo picture/file to root directory
            final Set<FileUpload> fileUploads = context.fileUploads();
            if (fileUploads.size() >= 1) {
                try {
                    FileUpload fileUpload = Iterators.getOnlyElement(fileUploads.iterator());
                    Path copied = Paths.get(logoFileName);
                    Path originalPath = Paths.get(fileUpload.uploadedFileName());
                    Files.copy(originalPath, copied, StandardCopyOption.REPLACE_EXISTING);
                } catch (IllegalArgumentException e) {
                    LOG.error("uploaded multiple images, expecting one");
                    throw new HandlerException(400, "Uploaded multiple images, expecting one");
                } catch (Exception e) {
                    LOG.error("unable to access upload file");
                    throw new HandlerException(400, "Unable to access uploaded file");
                }
                cleanUploads(context);
            }

            response
                .putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(customText));
        });
    }

    private void resetCustomLogoAndText(RoutingContext context) {
        auth(context, singletonList(User.Role.SYS_ADMIN), user -> {
            final File customTextFile = new File(customTextFileName);
            final File logoFile = new File(logoFileName);
            try {
                Files.deleteIfExists(customTextFile.toPath());
                Files.deleteIfExists(logoFile.toPath());
            } catch (IOException e) {
                LOG.error("Error deleting custom logo and text files");
                handleError(context, e);
                return;
            }
            ok(context, "Reset custom logo and text successful", context.normalisedPath());
        });
    }

    private void getCustomTextOnly(RoutingContext context) {
        // read the custom text object from file
        CustomText customText = null;
        final File existingCustomTextFile = new File(customTextFileName);
        final CustomText emptyCustomText = new CustomTextBuilder().build();
        if (!existingCustomTextFile.exists()) {
            context.response()
                .putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(emptyCustomText));
        } else {
            try {
                FileInputStream fi = new FileInputStream(existingCustomTextFile);
                ObjectInputStream oi = new ObjectInputStream(fi);

                customText = (CustomText) oi.readObject();

                oi.close();
                fi.close();
            } catch (FileNotFoundException e) {
                LOG.error("This should never happen! Custom Text File Not Found");
            } catch (IOException e) {
                LOG.error("Error initializing stream");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            context.response()
                .putHeader("content-type", "application/json; charset=utf-8")
                .end(Json.encode(customText == null ? emptyCustomText : customText));
        }
    }

    private void getLogoOnly(RoutingContext context) {
        File f = new File(logoFileName);
        if (!f.exists()) {
            final InputStream in = getClass().getClassLoader().getResourceAsStream("web/images/BSIlogo.png");
            try {
                final FileOutputStream out = new FileOutputStream(f);
                IOUtils.copy(in, out);
                in.close();
                out.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        final HttpServerResponse response = context.response();
        response.putHeader("Content-Type", "image/png");
        if(f.exists() && !f.isDirectory()) {
            response.sendFile(logoFileName);
        } else {
            response.end();
        }
    }
}
