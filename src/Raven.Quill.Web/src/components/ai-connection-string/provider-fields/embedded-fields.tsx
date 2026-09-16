import { EmbeddingsMaxConcurrentBatchesField } from "@/components/ai-connection-string/provider-fields/shared-fields";

// The embedded provider runs the bundled bge-micro-v2 model locally, so it only needs the
// shared throughput control.
export function EmbeddedFields() {
    return <EmbeddingsMaxConcurrentBatchesField baseName="embeddedSettings" />;
}
