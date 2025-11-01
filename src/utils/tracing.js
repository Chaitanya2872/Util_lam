// src/utils/tracing.js
import { NodeSDK } from '@opentelemetry/sdk-node';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { diag, DiagConsoleLogger, DiagLogLevel } from '@opentelemetry/api';


diag.setLogger(new DiagConsoleLogger(), DiagLogLevel.INFO);

const traceExporter = new OTLPTraceExporter({
  // Optional: configure collector endpoint
  // url: 'http://localhost:4318/v1/traces'
});

const sdk = new NodeSDK({
  traceExporter,
  instrumentations: [getNodeAutoInstrumentations()],
});

(async () => {
  try {
    await sdk.start();
    console.log('✅ OpenTelemetry initialized');
  } catch (err) {
    console.error('❌ OpenTelemetry init failed:', err);
  }
})();

process.on('SIGTERM', () => {
  sdk.shutdown()
    .then(() => console.log('Tracing shut down'))
    .catch((err) => console.error('Error shutting down tracing', err))
    .finally(() => process.exit(0));
});
