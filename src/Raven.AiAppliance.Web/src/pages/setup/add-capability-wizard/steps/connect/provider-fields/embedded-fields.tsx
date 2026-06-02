import { EmbeddingsMaxConcurrentBatchesField } from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/shared-fields";

// The embedded provider runs the bundled bge-micro-v2 model locally, so it only needs the
// shared throughput control.
export function EmbeddedFields() {
    return <EmbeddingsMaxConcurrentBatchesField baseName="embeddedSettings" />;
}
