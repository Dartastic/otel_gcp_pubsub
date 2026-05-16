// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_gcp_pubsub/otel_gcp_pubsub.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_gcp_pubsub-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('publish records PRODUCER span', () async {
    await tracedGcpPubsubPublish<String>(
      project: 'my-proj',
      topic: 'orders',
      payloadSize: 512,
      orderingKey: 'user-1',
      invoke: () async => const GcpPubsubPublishResult(
        value: 'ok',
        messageId: 'm-1',
      ),
    );
    final span = spans.findSpanByName('publish orders');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.producer);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'gcp_pubsub');
    expect(a['messaging.operation'], 'publish');
    expect(a['messaging.destination.name'], 'orders');
    expect(a['gcp.project_id'], 'my-proj');
    expect(a['messaging.message.body.size'], 512);
    expect(a['gcp.pubsub.ordering_key'], 'user-1');
    expect(a['messaging.message.id'], 'm-1');
  });

  test('receive records CONSUMER span', () async {
    await tracedGcpPubsubReceive<String>(
      project: 'my-proj',
      subscription: 'orders-worker',
      messageId: 'm-1',
      orderingKey: 'user-1',
      deliveryAttempt: 2,
      invoke: () async => 'processed',
    );
    final span = spans.findSpanByName('receive orders-worker');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.consumer);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'gcp_pubsub');
    expect(a['messaging.operation'], 'receive');
    expect(a['messaging.destination.name'], 'orders-worker');
    expect(a['messaging.message.id'], 'm-1');
    expect(a['gcp.pubsub.delivery_attempt'], 2);
  });

  test('records exception', () async {
    await expectLater(
      tracedGcpPubsubPublish<String>(
        project: 'p',
        topic: 't',
        invoke: () async => throw StateError('boom'),
      ),
      throwsA(isA<StateError>()),
    );
    expect(spans.findSpanByName('publish t')!.status, SpanStatusCode.Error);
  });

  test('suppression skips spans', () async {
    await runWithoutGcpPubsubInstrumentationAsync(() async {
      await tracedGcpPubsubPublish<String>(
        project: 'p',
        topic: 't',
        invoke: () async => const GcpPubsubPublishResult(value: 'v'),
      );
      await tracedGcpPubsubReceive<String>(
        project: 'p',
        subscription: 's',
        invoke: () async => 'ok',
      );
    });
    expect(spans.findSpansStartingWith('publish '), isEmpty);
    expect(spans.findSpansStartingWith('receive '), isEmpty);
  });
}
