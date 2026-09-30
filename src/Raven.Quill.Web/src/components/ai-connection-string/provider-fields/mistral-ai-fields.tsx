import { useFormContext } from "react-hook-form";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
import { AdvancedFields } from "@/components/ai-connection-string/provider-fields/advanced-fields";
import { EmbeddingsMaxConcurrentBatchesField } from "@/components/ai-connection-string/provider-fields/shared-fields";

const ENDPOINTS = ["https://api.mistral.ai/v1/"];
const MODELS = ["mistral-embed"];

export function MistralAiFields() {
    const { control, getValues } = useFormContext<ConnectionStringFormData>();

    const hasAdvancedValues = getValues("mistralAiSettings").embeddingsMaxConcurrentBatches != null;

    return (
        <>
            <FormInput
                control={control}
                name="mistralAiSettings.apiKey"
                label="API Key"
                type="password"
                placeholder="..."
            />
            <FormAutocomplete
                control={control}
                name="mistralAiSettings.endpoint"
                label="Endpoint"
                placeholder="https://api.mistral.ai/v1/"
                options={ENDPOINTS}
            />
            <FormAutocomplete
                control={control}
                name="mistralAiSettings.model"
                label="Model"
                placeholder="mistral-embed"
                options={MODELS}
            />
            <AdvancedFields defaultOpen={hasAdvancedValues}>
                <EmbeddingsMaxConcurrentBatchesField baseName="mistralAiSettings" />
            </AdvancedFields>
        </>
    );
}
