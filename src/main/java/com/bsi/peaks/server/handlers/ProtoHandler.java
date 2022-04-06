package com.bsi.peaks.server.handlers;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.event.Logging;
import akka.japi.function.Procedure;
import akka.japi.function.Procedure2;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.Supervision;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.bsi.peaks.io.writer.dto.Csv;
import com.bsi.peaks.model.dto.Project;
import com.bsi.peaks.server.handlers.helper.AckingReceiver;
import com.bsi.peaks.server.handlers.helper.VertxChunkedOutputStream;
import com.bsi.peaks.server.handlers.helper.VertxChunkedOutputStream.VertxChunkedOutputStreamClosed;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.util.JsonFormat;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static com.bsi.peaks.server.handlers.AnalysisProtoHandler.WEB_SOCKET_GROUP_SIZE;
import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.CompletableFuture.completedFuture;

public abstract class ProtoHandler extends ApiHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoHandler.class);
    protected final ActorSystem system;

    public ProtoHandler(
        Vertx vertx,
        final ActorSystem system
    ) {
        super(vertx);
        this.system = system;
    }

    public static <T extends GeneratedMessageV3> Sink<T, CompletionStage<Done>> json(
        String name,
        HttpServerResponse response
    ) {
        final JsonFormat.Printer printer = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .includingDefaultValueFields();
        return Flow.<T>create()
            .async()
            .map(x -> {
                try {
                    return printer.print(x);
                } catch (Throwable e) {
                    LOG.error("Error converting to JSON", e);
                    return "\"Error converting to JSON. See server log.\"";
                }
            })
            .buffer(256, OverflowStrategy.backpressure())
            .intersperse("[", ",", "]")
            .toMat(responseSink(
                response,
                r -> {
                    r.putHeader("Content-Disposition", "attachment; filename=\"" + name + ".json\"");
                    r.putHeader("Content-Type", "application/json");
                },
                (output, value) -> output.write(value.getBytes())
            ), Keep.right());
    }

    public static <T extends GeneratedMessageV3> Sink<T, CompletionStage<Done>> protobuf(
        String name,
        HttpServerResponse response
    ) {
        return Flow.<T>create()
            .buffer(256, OverflowStrategy.backpressure())
            .toMat(responseSink(
                response,
                r -> {
                    r.putHeader("Content-Disposition", "attachment; filename=\"" + name + ".protobuf\"");
                    r.putHeader("Content-Type", "application/x-protobuf");
                },
                (output, proto) -> proto.writeDelimitedTo(output)
            ), Keep.right());
    }

    protected static <T> Sink<T, CompletionStage<Done>> responseSink(
        HttpServerResponse response,
        Procedure<HttpServerResponse> setHeaders,
        Procedure2<OutputStream, T> write
    ) {
        return responseSink(response, setHeaders, null, write);
    }

    protected static <T> Sink<T, CompletionStage<Done>> responseSink(
        HttpServerResponse response,
        Procedure<HttpServerResponse> setHeaders,
        Procedure<OutputStream> writeFirst,
        Procedure2<OutputStream, T> write
    ) {
        return Sink.lazyInitAsync(
            () -> {
                setHeaders.apply(response);
                response.setChunked(true);
                final OutputStream output = new BufferedOutputStream(new VertxChunkedOutputStream(response));
                if (writeFirst != null) {
                    writeFirst.apply(output);
                }
                return completedFuture(Sink.<T>foreach(value -> {
                    if (response.closed()) {
                        throw new IOException("Response closed prematurely");
                    }
                    write.apply(output, value);
                }).mapMaterializedValue(futureDone -> futureDone.whenComplete((done, throwable) -> {
                    try {
                        output.flush();
                        output.close();
                    } catch (IOException e) {
                        LOG.error("Exception during close of output stream", e);
                    }
                    response.end();
                })));
            }).mapMaterializedValue(lazyFutureDone -> lazyFutureDone.thenCompose(futureDone -> {
                if (futureDone.isPresent()) {
                    return futureDone.get()
                        .exceptionally(throwable -> {
                            if (throwable instanceof VertxChunkedOutputStreamClosed) {
                                return Done.getInstance();
                            }
                            throw propagate(throwable);
                        });
                } else {
                    // Nothing was output.
                    if (writeFirst != null) {
                        try {
                            setHeaders.apply(response);
                            response.setChunked(true);
                            try (OutputStream output = new BufferedOutputStream(new VertxChunkedOutputStream(response))) {
                                writeFirst.apply(output);
                                output.flush();
                            } catch (IOException e) {
                                LOG.error("Exception during close of output stream", e);
                            }
                        } catch (Exception e) {
                            throw Throwables.propagate(e);
                        } finally {
                            response.end();
                        }
                    } else {
                        response.setStatusCode(204).end();
                    }
                    return completedFuture(Done.getInstance());
                }
            }
        ));
    }

    public static <T extends GeneratedMessageV3> void respondWebSocketBackPressure(ServerWebSocket webSocket, Source<T, NotUsed> source,
                                                                                   Materializer materializer, ActorSystem system,
                                                                                   ActorRef receiver, CompletableFuture<Done> futureDone) {
        Sink<Buffer, NotUsed> webSocketSink = Sink.actorRefWithAck(receiver,
            AckingReceiver.INITIALIZED,
            AckingReceiver.ACK,
            AckingReceiver.COMPLETED,
            AckingReceiver.StreamFailure::new
        );

        source
            .alsoToMat(Sink.fold(0, (c, n) -> c + 1), Keep.right())
            .grouped(WEB_SOCKET_GROUP_SIZE)
            .map(group -> {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                group.forEach(item -> {
                    try {
                        item.writeDelimitedTo(out);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                out.flush();
                return Buffer.buffer(out.toByteArray());
            })
            .async()
            .toMat(
                webSocketSink, (futureCount, notUsed) -> futureCount.thenCombine(futureDone, (count, done) -> count)
            )
            .run(materializer)
            .whenComplete((count, error) -> {
                if (error == null) {
                    webSocket.writeFinalTextFrame(count.toString());
                    webSocket.close();
                } else if (error instanceof WebSocketExcpetion) {
                    final WebSocketExcpetion excpetion = (WebSocketExcpetion) error;
                    webSocket.close(excpetion.getErrorCode(), error.getMessage());
                    LOG.error("Error while using web socket (" + excpetion.getErrorCode() + "): " + webSocket.path(), error);
                } else {
                    webSocket.close((short) 1001, error.getMessage());
                    LOG.error("Error while using web socket: " + webSocket.path(), error);
                }
                system.stop(receiver);
            });
    }

    public static <T extends GeneratedMessageV3> void respondWebSocket(ServerWebSocket webSocket, Source<T, NotUsed> source,
                                                                       Materializer materializer) {
        source
            .alsoToMat(Sink.fold(0, (c, n) -> c + 1), Keep.right())
            .grouped(WEB_SOCKET_GROUP_SIZE)
            .map(group -> {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                group.forEach(item -> {
                    try {
                        item.writeDelimitedTo(out);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                out.flush();
                return Buffer.buffer(out.toByteArray());
            })
            .async()
            .toMat(
                Sink.foreach(webSocket::writeFinalBinaryFrame),
                (futureCount, futureDone) -> futureDone.thenCombine(futureCount, (done, count) -> count)
            )
            .run(materializer)
            .whenComplete((count, error) -> {
                if (error == null) {
                    webSocket.writeFinalTextFrame(count.toString());
                    webSocket.close();
                } else if (error instanceof WebSocketExcpetion) {
                    final WebSocketExcpetion excpetion = (WebSocketExcpetion) error;
                    webSocket.close(excpetion.getErrorCode(), error.getMessage());
                    LOG.error("Error while using web socket (" + excpetion.getErrorCode() + "): " + webSocket.path(), error);
                } else {
                    webSocket.close((short) 1001, error.getMessage());
                    LOG.error("Error while using web socket: " + webSocket.path(), error);
                }
            });
    }

    protected <T extends GeneratedMessageV3> CompletionStage<Done> serializationProcedure(
        HttpServerRequest request,
        HttpServerResponse response,
        Source<T, NotUsed> source,
        String name,
        boolean supportCsv,
        CompletionStage<? extends Csv<T>> csvHandler
    ) {
        final Materializer materializer = materializerWithErrorLogging(request);

        for (String accept : request.headers().getAll("Accept")) {
            if (accept.contains("protobuf")) {
                return source.runWith(AnalysisProtoHandler.protobuf(name, response), materializer);
            } else if (accept.contains("json")) {
                return source.runWith(AnalysisProtoHandler.json(name, response), materializer);
            } else if (accept.contains("csv")) {
                return csvHandler
                    .thenApply(csv -> csv(name, csv, response))
                    .thenCompose(sink -> source.runWith(sink, materializer));
            }
        }
        if (!supportCsv) {
            return source.runWith(AnalysisProtoHandler.protobuf(name, response), materializer);
        } else {
            return csvHandler
                .thenApply(csv -> csv(name, csv, response))
                .thenCompose(sink -> source.runWith(sink, materializer));
        }
    }

    protected <T extends GeneratedMessageV3> CompletionStage<Done> serializationProcedure(
        HttpServerRequest request,
        HttpServerResponse response,
        Source<T, NotUsed> source,
        String name
    ) {
        return serializationProcedure(
            request,
            response,
            source,
            name,
            false,
            Futures.failedCompletionStage(new IllegalStateException("This endpoint does not support csv."))
        );
    }

    protected <T extends GeneratedMessageV3> CompletionStage<Done> serializationProcedure(
        HttpServerRequest request,
        HttpServerResponse response,
        Source<T, NotUsed> source,
        Project project,
        String extensionName,
        boolean supportCsv,
        CompletionStage<? extends Csv<T>> csvHandler
    ) {
        return serializationProcedure(
            request,
            response,
            source,
            project.getName() + "." + extensionName,
            supportCsv,
            csvHandler
        );
    }

    protected <T extends GeneratedMessageV3> Sink<T, CompletionStage<Done>> csv(
        String name,
        Csv<T> csv,
        HttpServerResponse response
    ) {
        Sink<String, CompletionStage<Done>> sink = responseSink(
            response,
            r -> {
                r.putHeader("Content-Disposition", "attachment; filename=\"" + name + ".csv\"");
                r.putHeader("Content-Type", "text/csv");
            },
            output -> {
                output.write(csv.header().getBytes());
                output.write('\n');
            },
            (output, value) -> {
                output.write(value.getBytes());
                output.write('\n');
            }
        );

        return csv(csv, sink);
    }

    protected <T extends GeneratedMessageV3> Sink<T, CompletionStage<Done>> csv(
        Csv<T> csv,
        Sink<String, CompletionStage<Done>> responseSink
    ) {
        return Flow.<T>create()
            .async()
            .mapConcat(x -> {
                try {
                    return csv.rows(x);
                } catch (Throwable e) {
                    LOG.error("Exception during CSV conversion", e);
                    return ImmutableList.of("Error: " + e.getMessage());
                }
            })
            .buffer(256, OverflowStrategy.backpressure())
            .toMat(responseSink, Keep.right());
    }

    protected void respondWithProtobufSource(RoutingContext context, String name, Source<? extends GeneratedMessageV3, NotUsed> source) {
        try {
            final ActorMaterializerSettings settings = ActorMaterializerSettings.create(system)
                .withSupervisionStrategy((akka.japi.function.Function<Throwable, Supervision.Directive>) e -> {
                    LOG.error("Error occurred during export", e);
                    return Supervision.stop();
                });
            final Materializer materializer = ActorMaterializer.create(settings, system);

            final HttpServerRequest request = context.request();
            final HttpServerResponse response = context.response();
            CompletionStage<Done> futureDone = null;
            for (String accept : request.headers().getAll("Accept")) {
                if (accept.contains("protobuf")) {
                    futureDone = source.runWith(protobuf(name, response), materializer);
                } else if (accept.contains("json")) {
                    futureDone = source.runWith(json(name, response), materializer);
                }
            }
            if (futureDone == null) {
                futureDone = source.runWith(json(name, response), materializer);
            }

            futureDone.whenComplete((done, error) -> {
                if (error != null) {
                    handleError(context, error);
                } else {
                    response.end();
                }
            });

        } catch (Exception e) {
            context.fail(e);
        }
    }

    protected Materializer materializerWithErrorLogging(HttpServerRequest request) {
        final ActorMaterializerSettings settings = ActorMaterializerSettings.create(system)
            .withSupervisionStrategy((akka.japi.function.Function<Throwable, Supervision.Directive>) e -> {
                Logging.getLogger(system, this.getClass())
                    .error(e, "Error during stream processing: " + request.method() + " " + request.path());
                return Supervision.stop();
            });
        return ActorMaterializer.create(settings, system);
    }
}
