import { useFormContext } from "react-hook-form";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormTextarea } from "@/components/form/form-textarea";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
import { AdvancedFields } from "@/components/ai-connection-string/provider-fields/advanced-fields";
import { EmbeddingsMaxConcurrentBatchesField } from "@/components/ai-connection-string/provider-fields/shared-fields";

const MODELS = ["gemini-embedding-001", "text-embedding-005", "text-multilingual-embedding-002"];

const AI_VERSION_OPTIONS: FormSelectOption<ConnectionStringFormData["vertexSettings"]["aiVersion"]>[] = [
    { value: "V1", label: "V1" },
    { value: "V1_Beta", label: "V1 Beta" },
];

export function VertexFields() {
    const { control, getValues } = useFormContext<ConnectionStringFormData>();

    const settings = getValues("vertexSettings");
    const hasAdvancedValues = Boolean(settings.aiVersion || settings.embeddingsMaxConcurrentBatches != null);

    return (
        <>
            <FormTextarea
                control={control}
                name="vertexSettings.googleCredentialsJson"
                label="Google Credentials JSON"
                rows={8}
                placeholder='{ "type": "service_account", ... }'
                description="Service account credentials used to authenticate with Vertex AI."
            />
            <FormAutocomplete
                control={control}
                name="vertexSettings.model"
                label="Model"
                placeholder="gemini-embedding-001"
                options={MODELS}
            />
            <FormInput
                control={control}
                name="vertexSettings.location"
                label="Location"
                placeholder="us-central1"
                description="The Google Cloud region where your Vertex AI resource is deployed."
            />
            <AdvancedFields defaultOpen={hasAdvancedValues}>
                <FormSelect
                    control={control}
                    name="vertexSettings.aiVersion"
                    label="AI Version (optional)"
                    placeholder="Default"
                    options={AI_VERSION_OPTIONS}
                />
                <EmbeddingsMaxConcurrentBatchesField baseName="vertexSettings" />
            </AdvancedFields>
        </>
    );
}
