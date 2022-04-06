package com.bsi.peaks.server.handlers;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.Materializer;
import akka.stream.Supervision;
import com.bsi.peaks.model.DataModel;
import com.bsi.peaks.model.system.User;
import com.bsi.peaks.server.models.PageResult;
import com.bsi.peaks.server.models.Pagination;
import com.bsi.peaks.server.models.PaginationBuilder;
import com.bsi.peaks.server.service.CRUDService;
import com.google.inject.Inject;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

abstract class EsCRUDHandler<T extends DataModel> extends ApiHandler {
    protected final ActorSystem system;
    private static final Logger LOG = LoggerFactory.getLogger(EsCRUDHandler.class);
    private final Class<T> dataType;
    private final CRUDService<T> service;

    @Inject
    public EsCRUDHandler(Vertx vertx, final ActorSystem system, CRUDService<T> service, Class<T> dataType) {
        super(vertx);
        this.system = system;
        this.dataType = dataType;
        this.service = service;
    }

    @Override
    public Router createSubRouter() {
        Router subRouter = Router.router(vertx);
        subRouter.route("/").handler(BodyHandler.create());
        subRouter.route("/:id").handler(BodyHandler.create());
        subRouter.post("/").consumes("*/json").handler(this::create);
        subRouter.get("/").handler(this::readAll);
        subRouter.put("/:id").consumes("*/json").handler(this::update);
        subRouter.delete("/:id").handler(this::delete);
        return subRouter;
    }

    abstract String dataId(T t);
    abstract String dataName(T t);
    abstract UUID creatorId(T t);

    protected CRUDService<T> service(){
        return service;
    }

    private void readAll(RoutingContext context) {
        auth(context, user ->
            service().readById(user).whenComplete((items, error) -> {
                if (error != null) {
                    LOG.error("Unable to load data from " + dataType.toString() + "  database", error);
                    context.fail(error);
                } else {
                    Pagination pagination = new PaginationBuilder()
                        .page(0)
                        .pageSize(items.size())
                        .totalPages(0)
                        .totalItems(items.size())
                        .build();
                    ok(context, new PageResult(items, pagination));
                }
            }));
    }

    private void create(RoutingContext context) {
        auth(context, user -> {
            T data;
            try {
                data = DataModel.fromJSON(context.getBodyAsString(), dataType);
            } catch (Exception e) {
                badRequest(context, "Unable to parse request " + context.getBodyAsString());
                return;
            }
            service().create(data, user).whenComplete((id, error) -> {
                if (error != null) {
                    LOG.error("Unable to save " + dataName(data) + " to database: " + error.getMessage(),
                        error);
                    context.fail(error);
                } else {
                    ok(context, dataId(data), context.normalisedPath() + "/" + dataId(data));
                }
            });
        });
    }

    private void delete(RoutingContext context) {
        auth(context, user -> {
            String key = context.request().getParam("id");
            if (key != null) {
                service().delete(key, user).whenComplete((num, error) -> {
                    if (error != null) {
                        LOG.error("Unable to delete " + key + " from database", error);
                        context.fail(error);
                    } else {
                        ok(context);
                    }
                });
            } else {
                badRequest(context, "Invalid parameter id");
            }
        });
    }

    protected void update(RoutingContext context) {
        auth(context, user -> {
            String key = context.request().getParam("id");
            if (key != null) {
                T data;
                try {
                    data = DataModel.fromJSON(context.getBodyAsString(), dataType);
                } catch (Exception e) {
                    badRequest(context, "Unable to parse request " + context.getBodyAsString());
                    return;
                }
                service().readByNames(Arrays.asList(dataName(data))).whenComplete((oldDataList, error) -> {
                    if (error != null || oldDataList.size() != 1) {
                        LOG.error("Unable find " + key + " in database", error);
                        context.fail(error);
                    } else {
                        T oldData = oldDataList.get(0);
                        if (creatorId(oldData).equals(creatorId(data))) { //no permission change -> update
                            updateData(context, user, data, key);
                        } else { //permission change
                            updateOwner(context, user, data, key);
                        }
                    }
                });
            } else {
                badRequest(context, "Invalid parameter id");
            }
        });
    }

    protected void updateOwner(RoutingContext context, User user, T data, String key) {
        service().grantPermission(dataId(data), user.userId(), creatorId(data)).whenComplete((updated, error) -> {
            if (error != null) {
                LOG.error("Unable to grant permission " + key + " in database", error);
                context.fail(error);
            } else {
                ok(context, data);
            }
        });
    }

    protected void updateData(RoutingContext context, User user, T data, String key) {
        service().update(key, data, user).whenComplete((updated, error) -> {
            if (error != null) {
                LOG.error("Unable to update " + key + " in database", error);
                context.fail(error);
            } else {
                ok(context, data);
            }
        });
    }

}
