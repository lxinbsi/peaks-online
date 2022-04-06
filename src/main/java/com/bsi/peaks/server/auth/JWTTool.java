package com.bsi.peaks.server.auth;

import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.config.SystemConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jwt.JWT;
import io.vertx.ext.jwt.JWTOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;

/**
 * @author Shengying Pan Created by span on 2/9/17.
 */
@Singleton
public class JWTTool {
    private static final Logger LOG = LoggerFactory.getLogger(JWTTool.class);
    private final JWT core;

    private final ObjectMapper OM = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .registerModule(new Jdk8Module());

    @SuppressWarnings("ConstantConditions")
    @Inject
    public JWTTool(SystemConfig config) {
        try {
            KeyStore ks = KeyStore.getInstance("jceks");
            Buffer keystore = readKeystore();
            try (InputStream in = new ByteArrayInputStream(keystore.getBytes())) {
                ks.load(in, config.getServerJWTSecret().toCharArray());
            }
            this.core = new JWT(ks, config.getServerJWTSecret().toCharArray());
        } catch (Exception e) {
            LOG.error("Creating JWT core failed", e);
            throw new RuntimeException("Creating JWT core failed", e);
        }
    }

    private Buffer readKeystore() {
        try (InputStream in = getClass().getClassLoader().getResourceAsStream("keystore.jceks")) {
            byte[] buffer = new byte[1024];
            int pos = 0;
            while (true) {
                int ch = in.read();
                if (ch < 0) {
                    break;
                } else {
                    buffer[pos++] = (byte) ch;
                    if (pos == buffer.length) {
                        buffer = Arrays.copyOf(buffer, pos * 2);
                    }
                }
            }
            if (pos == 0) {
                throw new IllegalStateException("Nothing to read from keystore.jceks");
            } else {
                return Buffer.buffer(Arrays.copyOf(buffer, pos));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to load keystore.jceks from file system");
        }
    }

    public Date decodeTime(String jwt) {
        JsonObject payload = core.decode(jwt);
        String time = payload.getString("time");
        return new Date(Long.parseLong(time));
    }

    public Instant decodeInstant(String jwt) {
        JsonObject payload = core.decode(jwt);
        String time = payload.getString("time");
        return Instant.ofEpochMilli(Long.parseLong(time));
    }

    public String encodeInstant(Date time) {
        JsonObject payload = new JsonObject().put("time", Long.toString(time.getTime()));
        return core.sign(payload, new JWTOptions());
    }

    public String encodeInstant(Instant time) {
        JsonObject payload = new JsonObject().put("time", Long.toString(time.toEpochMilli()));
        return core.sign(payload, new JWTOptions());
    }

    public User decode(String jwt) {
        if (jwt == null) {
            return null;
        } else {
            JsonObject payload;
            try {
                payload = core.decode(jwt);
            } catch (Exception e) {
                LOG.error("Unable to decode jwt " + jwt, e);
                return null;
            }

            String user = payload.getString("user", null);
            if (user == null) {
                return null;
            } else {
                try {
                    return OM.readValue(user, User.class);
                } catch (IOException e) {
                    LOG.error("Unable to decode jwt " + jwt, e);
                    return null;
                }
            }
        }
    }
    
    public String encode(User user) {
        JsonObject payload = null;
        try {
            payload = new JsonObject().put("user", OM.writeValueAsString(user));
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
        return core.sign(payload, new JWTOptions());
    }
}
