// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_gcp_pubsub/otel_gcp_pubsub.dart';

/// Traces Gcp Pubsub calls with OpenTelemetry.
///
/// `otel_gcp_pubsub` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Gcp Pubsub call in
/// [tracedGcpPubsubPublish]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'gcp-pubsub-example');

  // Wraps a GCP Pub/Sub publish in a PRODUCER span using OTel messaging semconv (messaging.system=gcp_pubsub).
  final gcpPubsubPublishId = await tracedGcpPubsubPublish<String>(
    project: 'example',
    topic: 'orders',
    payloadSize: 512,
    orderingKey: 'orders/2026/inv-1042.pdf',
    invoke: () async {
      // Make the Gcp Pubsub call here, then report what it returned.
      const id = 'gcp_pubsub_8f3a2c';
      return const GcpPubsubPublishResult(
          value: id, messageId: 'message_8f3a2c');
    },
  );
  OTelLog.info('tracedGcpPubsubPublish returned $gcpPubsubPublishId');

  // Wraps the user's handling of one received Pub/Sub message in a CONSUMER span.
  final gcpPubsubReceiveId = await tracedGcpPubsubReceive<String>(
    project: 'example',
    subscription: 'example',
    messageId: 'message_8f3a2c',
    orderingKey: 'orders/2026/inv-1042.pdf',
    deliveryAttempt: 1,
    invoke: () async {
      // Make the Gcp Pubsub call here, then report what it returned.
      return 'gcp_pubsub_8f3a2c';
    },
  );
  OTelLog.info('tracedGcpPubsubReceive returned $gcpPubsubReceiveId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutGcpPubsubInstrumentationAsync.
  await runWithoutGcpPubsubInstrumentationAsync(
    () => tracedGcpPubsubPublish<String>(
      project: 'example',
      topic: 'orders',
      payloadSize: 512,
      orderingKey: 'orders/2026/inv-1042.pdf',
      invoke: () async {
        const id = 'gcp_pubsub_8f3a2c';
        return const GcpPubsubPublishResult(
            value: id, messageId: 'message_8f3a2c');
      },
    ),
  );

  await OTel.shutdown();
}
