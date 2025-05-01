package net.pincette.letterbox.kafka;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Instant.now;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Logger.getLogger;
import static net.pincette.jes.JsonFields.CORR;
import static net.pincette.jes.JsonFields.ID;
import static net.pincette.jes.Util.getUsername;
import static net.pincette.jes.tel.OtelUtil.attributes;
import static net.pincette.jes.tel.OtelUtil.metrics;
import static net.pincette.jes.util.Kafka.createReliableProducer;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.netty.http.Util.simpleResponse;
import static net.pincette.netty.http.Util.wrapMetrics;
import static net.pincette.netty.http.Util.wrapTracing;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.FlattenList.flattenList;
import static net.pincette.rs.LambdaSubscriber.lambdaSubscriber;
import static net.pincette.rs.Probe.probeValue;
import static net.pincette.rs.Util.empty;
import static net.pincette.rs.json.Util.parseJson;
import static net.pincette.rs.kafka.KafkaSubscriber.subscriber;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Collections.put;
import static net.pincette.util.Util.getStackTrace;
import static net.pincette.util.Util.must;
import static net.pincette.util.Util.tryToDoSilent;

import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.opentelemetry.api.metrics.Meter;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.jes.tel.EventTrace;
import net.pincette.jes.tel.HttpMetrics;
import net.pincette.jes.tel.OtelUtil;
import net.pincette.jes.util.Kafka;
import net.pincette.json.JsonUtil;
import net.pincette.kafka.json.JsonSerializer;
import net.pincette.netty.http.Metrics;
import net.pincette.netty.http.RequestHandler;
import net.pincette.rs.DequePublisher;
import net.pincette.rs.Source;
import net.pincette.util.Util.PredicateException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Publisher implements AutoCloseable {
  private static final String ANONYMOUS = "anonymous";
  private static final String AS_STRING = "asString";
  private static final String HTTP_SERVER_LETTER_BOX_MESSAGES = "http.server.letter_box_messages";
  private static final String INSTANCE_ATTRIBUTE = "instance";
  private static final String KAFKA = "kafka";
  private static final Logger LOGGER = getLogger("net.pincette.letterbox.kafka");
  private static final String MESSAGE_TOPIC = "topic";
  private static final String NAMESPACE = "namespace";
  private static final String TRACES_TOPIC = "tracesTopic";
  private static final String TRACE_ID_FIELD = "traceId";

  private final boolean asString;
  private final Config config;
  private final Set<AutoCloseable> counters = new HashSet<>();
  private final EventTrace eventTrace;
  private final String instance;
  private final Map<String, Object> kafkaConfig;
  private final String serviceName;
  private final String serviceNamespace;
  private final String serviceVersion;
  private final String topic;
  private final String tracesTopic;
  private final Function<Context, Map<String, String>> telemetryAttributes;
  private final Function<Context, List<JsonObject>> transform;
  private final Predicate<Context> verify;

  private Publisher(
      final String serviceName,
      final String serviceVersion,
      final Predicate<Context> verify,
      final Function<Context, List<JsonObject>> transform,
      final String instance,
      final Function<Context, Map<String, String>> telemetryAttributes,
      final Config config) {
    this.config = config;
    this.asString = configValue(Config::getBoolean, AS_STRING).orElse(false);
    this.instance = instance != null ? instance : randomUUID().toString();
    this.kafkaConfig = configValue(Config::getConfig, KAFKA).map(Kafka::fromConfig).orElse(null);
    this.serviceNamespace = configValue(Config::getString, NAMESPACE).orElse(null);
    this.serviceName = serviceName;
    this.serviceVersion = serviceVersion;
    this.verify = verify;
    this.transform = transform != null ? transform : Publisher::noTransformation;
    this.topic = configValue(Config::getString, MESSAGE_TOPIC).orElse(null);
    this.tracesTopic = configValue(Config::getString, TRACES_TOPIC).orElse(null);
    this.telemetryAttributes =
        telemetryAttributes != null ? telemetryAttributes : pair -> new HashMap<>();

    eventTrace =
        tracesTopic != null
            ? new EventTrace()
                .withServiceNamespace(serviceNamespace)
                .withServiceName(serviceName)
                .withServiceVersion(serviceVersion)
                .withName(serviceName)
            : null;
  }

  public Publisher() {
    this(null, null, null, null, null, null, null);
  }

  private static String idValue(final JsonObject json, final String field) {
    return ofNullable(json.getString(field, null)).orElseGet(() -> randomUUID().toString());
  }

  private static Subscriber<Metrics> metricsSubscriber(final Meter meter, final String instance) {
    return HttpMetrics.subscriber(meter, path -> null, instance);
  }

  private static List<JsonObject> noTransformation(final Context context) {
    return list(context.message);
  }

  private static KafkaProducer<String, JsonObject> producer(final Map<String, Object> kafkaConfig) {
    return createReliableProducer(kafkaConfig, new StringSerializer(), new JsonSerializer());
  }

  private static KafkaProducer<String, String> producerString(
      final Map<String, Object> kafkaConfig) {
    return createReliableProducer(kafkaConfig, new StringSerializer(), new StringSerializer());
  }

  private static Flow.Publisher<ByteBuf> reportException(
      final HttpResponse response, final Throwable t) {
    LOGGER.log(SEVERE, t, t::getMessage);
    response.setStatus(t instanceof PredicateException ? FORBIDDEN : BAD_REQUEST);

    return Source.of(wrappedBuffer(getStackTrace(t).getBytes(UTF_8)));
  }

  private static CompletionStage<Flow.Publisher<ByteBuf>> response(
      final HttpResponse response, final HttpResponseStatus status) {
    return simpleResponse(response, status, empty());
  }

  private static JsonObject updateMessage(final JsonObject json) {
    return createObjectBuilder(json)
        .add(ID, idValue(json, ID))
        .add(CORR, idValue(json, CORR))
        .build();
  }

  public void close() {
    counters.forEach(c -> tryToDoSilent(c::close));
  }

  private Consumer<Context> counter(final Meter meter) {
    return OtelUtil.counter(
        meter,
        HTTP_SERVER_LETTER_BOX_MESSAGES,
        context -> attributes(telemetryAttributes.apply(context)),
        context -> 1L,
        counters);
  }

  private RequestHandler handler(final Meter meter) {
    final Consumer<Context> counter = meter != null ? counter(meter) : null;
    final Deque<JsonObject> publisher = publisher();

    return (request, requestBody, response) ->
        Optional.of(request.method())
            .filter(m -> m.equals(POST))
            .map(
                doms ->
                    readMessage(request, requestBody, publisher, counter)
                        .thenComposeAsync(
                            t ->
                                t == null
                                    ? response(response, ACCEPTED)
                                    : completedFuture(reportException(response, t))))
            .orElseGet(
                () ->
                    response(
                        response, !request.method().equals(POST) ? NOT_IMPLEMENTED : FORBIDDEN));
  }

  private <T> Optional<T> configValue(final BiFunction<Config, String, T> fn, final String path) {
    return ofNullable(config)
        .flatMap(c -> net.pincette.config.Util.configValue(p -> fn.apply(c, p), path));
  }

  private <T> ProducerRecord<String, T> publishMessage(
      final JsonObject json, final Function<JsonObject, T> mapper) {
    return tracesTopic != null && json.containsKey(TRACE_ID_FIELD)
        ? new ProducerRecord<>(tracesTopic, json.getString(TRACE_ID_FIELD), mapper.apply(json))
        : new ProducerRecord<>(topic, json.getString(ID), mapper.apply(json));
  }

  private Deque<JsonObject> publisher() {
    return asString
        ? publisher(v -> string(v, false), () -> producerString(kafkaConfig))
        : publisher(v -> v, () -> producer(kafkaConfig));
  }

  private <T> Deque<JsonObject> publisher(
      final Function<JsonObject, T> mapper, final Supplier<KafkaProducer<String, T>> producer) {
    final DequePublisher<JsonObject> dequePublisher = new DequePublisher<>();

    with(dequePublisher).map(v -> publishMessage(v, mapper)).get().subscribe(subscriber(producer));

    return dequePublisher.getDeque();
  }

  private CompletionStage<Throwable> readMessage(
      final HttpRequest request,
      final Flow.Publisher<ByteBuf> requestBody,
      final Deque<JsonObject> publisher,
      final Consumer<Context> counter) {
    final CompletableFuture<Throwable> future = new CompletableFuture<>();

    with(requestBody)
        .map(ByteBuf::nioBuffer)
        .map(parseJson())
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(json -> must(json, j -> verify.test(new Context(request, j))))
        .map(json -> transform.apply(new Context(request, json)))
        .map(flattenList())
        .map(Publisher::updateMessage)
        .map(
            probeValue(
                m -> {
                  if (counter != null) {
                    counter.accept(new Context(request, m));
                  }
                }))
        .map(
            json ->
                eventTrace != null
                    ? list(json, traceMessage(new Context(request, json)))
                    : list(json))
        .map(flattenList())
        .get()
        .subscribe(
            lambdaSubscriber(publisher::addFirst, () -> future.complete(null), future::complete));

    return future;
  }

  public RequestHandler requestHandler() {
    return wrapTracing(
        metrics(serviceNamespace, serviceName, serviceVersion, config)
            .map(m -> m.getMeter(serviceName))
            .map(m -> wrapMetrics(handler(m), metricsSubscriber(m, instance)))
            .orElseGet(() -> handler(null)),
        LOGGER);
  }

  private JsonObject traceMessage(final Context context) {
    return eventTrace
        .withTraceId(context.message.getString(CORR))
        .withTimestamp(now())
        .withAttributes(put(telemetryAttributes.apply(context), INSTANCE_ATTRIBUTE, instance))
        .withUsername(getUsername(context.message).orElse(ANONYMOUS))
        .toJson()
        .build();
  }

  public Publisher withConfig(final Config config) {
    return new Publisher(
        serviceName, serviceVersion, verify, transform, instance, telemetryAttributes, config);
  }

  public Publisher withInstance(final String instance) {
    return new Publisher(
        serviceName, serviceVersion, verify, transform, instance, telemetryAttributes, config);
  }

  public Publisher withServiceName(final String serviceName) {
    return new Publisher(
        serviceName, serviceVersion, verify, transform, instance, telemetryAttributes, config);
  }

  public Publisher withServiceVersion(final String serviceVersion) {
    return new Publisher(
        serviceName, serviceVersion, verify, transform, instance, telemetryAttributes, config);
  }

  public Publisher withTelemetryAttributes(
      final Function<Context, Map<String, String>> telemetryAttributes) {
    return new Publisher(
        serviceName, serviceVersion, verify, transform, instance, telemetryAttributes, config);
  }

  public Publisher withTransform(final Function<Context, List<JsonObject>> transform) {
    return new Publisher(
        serviceName, serviceVersion, verify, transform, instance, telemetryAttributes, config);
  }

  public Publisher withVerify(final Predicate<Context> verify) {
    return new Publisher(
        serviceName, serviceVersion, verify, transform, instance, telemetryAttributes, config);
  }

  public record Context(HttpRequest request, JsonObject message) {}
}
