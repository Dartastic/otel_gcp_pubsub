// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_gcp_pubsub_suppression.dart';

const _tracerName = 'dartastic_otel_libs_gcp_pubsub';
const _systemValue = 'gcp_pubsub';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Pair returned from `tracedGcpPubsubPublish` callback.
class GcpPubsubPublishResult<T> {
  const GcpPubsubPublishResult({required this.value, this.messageId});
  final T value;
  final String? messageId;
}

/// Wraps a GCP Pub/Sub publish in a PRODUCER span using OTel
/// messaging semconv (messaging.system=gcp_pubsub).
Future<T> tracedGcpPubsubPublish<T>({
  required String project,
  required String topic,
  required Future<GcpPubsubPublishResult<T>> Function() invoke,
  int? payloadSize,
  String? orderingKey,
}) async {
  if (gcpPubsubInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'publish',
    'messaging.destination.name': topic,
    'gcp.project_id': project,
  };
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (orderingKey != null) m['gcp.pubsub.ordering_key'] = orderingKey;

  final span = _tracer().startSpan(
    'publish $topic',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.messageId != null) {
      span.addAttributes(OTel.attributes([
        OTel.attributeString('messaging.message.id', r.messageId!),
      ]));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps the user's handling of one received Pub/Sub message in a
/// CONSUMER span. Call this *inside* the subscriber callback,
/// after you receive a message but before `ack()`/`nack()`. The
/// span name pattern is OTel-spec: `receive <subscription>`.
Future<T> tracedGcpPubsubReceive<T>({
  required String project,
  required String subscription,
  required Future<T> Function() invoke,
  String? messageId,
  String? orderingKey,
  int? deliveryAttempt,
}) async {
  if (gcpPubsubInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'receive',
    'messaging.destination.name': subscription,
    'gcp.project_id': project,
  };
  if (messageId != null) m['messaging.message.id'] = messageId;
  if (orderingKey != null) m['gcp.pubsub.ordering_key'] = orderingKey;
  if (deliveryAttempt != null) {
    m['gcp.pubsub.delivery_attempt'] = deliveryAttempt;
  }

  final span = _tracer().startSpan(
    'receive $subscription',
    kind: SpanKind.consumer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
