import { useFormContext } from "react-hook-form";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
import { EmbeddingsMaxConcurrentBatchesField } from "@/components/ai-connection-string/provider-fields/shared-fields";

const ENDPOINTS = ["https://api-inference.huggingface.com/"];

export function HuggingFaceFields() {
    const { control } = useFormContext<ConnectionStringFormData>();

    return (
        <>
            <FormInput
                control={control}
                name="huggingFaceSettings.apiKey"
                label="API Key"
                type="password"
                placeholder="hf_..."
            />
            <FormAutocomplete
                control={control}
                name="huggingFaceSettings.endpoint"
                label="Endpoint (optional)"
                placeholder="https://api-inference.huggingface.com/"
                options={ENDPOINTS}
            />
            <FormInput
                control={control}
                name="huggingFaceSettings.model"
                label="Model"
                placeholder="sentence-transformers/all-MiniLM-L6-v2"
            />
            <EmbeddingsMaxConcurrentBatchesField baseName="huggingFaceSettings" />
        </>
    );
}
