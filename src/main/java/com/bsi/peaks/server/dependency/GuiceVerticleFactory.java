package com.bsi.peaks.server.dependency;

import com.google.inject.Injector;
import io.vertx.core.Verticle;
import io.vertx.core.impl.verticle.CompilingClassLoader;
import io.vertx.core.spi.VerticleFactory;

/**
 * @author Shengying Pan
 * Created by span on 1/12/17.
 */
public class GuiceVerticleFactory implements VerticleFactory {
    public static final String PREFIX = "guice";
    private final Injector injector;

    public GuiceVerticleFactory(Injector injector) {
        this.injector = injector;
    }

    @Override
    public String prefix() {
        return PREFIX;
    }

    @Override
    public Verticle createVerticle(String verticleName, ClassLoader classLoader) throws Exception {
        verticleName = VerticleFactory.removePrefix(verticleName);

        Class clazz;
        if (verticleName.endsWith(".java")) {
            CompilingClassLoader compilingClassLoader = new CompilingClassLoader(classLoader, verticleName);
            String className = compilingClassLoader.resolveMainClassName();
            clazz = compilingClassLoader.loadClass(className);
        } else {
            clazz = classLoader.loadClass(verticleName);
        }

        return (Verticle) injector.getInstance(clazz);
    }
}
