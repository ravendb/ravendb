import { useFormContext } from "react-hook-form";
import type { AiModelType } from "@/api/generated/server-api";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
import { AdvancedFields } from "@/components/ai-connection-string/provider-fields/advanced-fields";
import {
    DimensionsField,
    EmbeddingsMaxConcurrentBatchesField,
    PromptCacheField,
} from "@/components/ai-connection-string/provider-fields/shared-fields";

const CHAT_MODELS = ["gemini-3-pro-preview", "gemini-3-flash-preview"];
const EMBEDDINGS_MODELS = ["text-embedding-004", "text-embedding-005", "text-multilingual-embedding-002"];
const ENDPOINTS = ["https://generativelanguage.googleapis.com"];

const AI_VERSION_OPTIONS: FormSelectOption<ConnectionStringFormData["googleSettings"]["aiVersion"]>[] = [
    { value: "V1", label: "V1" },
    { value: "V1_Beta", label: "V1 Beta" },
];

export function GoogleFields({ modelType }: { modelType: AiModelType }) {
    const { control, getValues } = useFormContext<ConnectionStringFormData>();
    const isChat = modelType === "Chat";

    const settings = getValues("googleSettings");
    const hasAdvancedValues = Boolean(
        settings.aiVersion ||
        settings.endpoint ||
        settings.dimensions != null ||
        settings.embeddingsMaxConcurrentBatches != null,
    );

    return (
        <>
            <FormInput
                control={control}
                name="googleSettings.apiKey"
                label="API Key"
                type="password"
                placeholder="..."
            />
            <FormAutocomplete
                control={control}
                name="googleSettings.model"
                label="Model"
                placeholder="gemini-3-flash-preview, …"
                options={isChat ? CHAT_MODELS : EMBEDDINGS_MODELS}
            />
            <AdvancedFields defaultOpen={hasAdvancedValues}>
                <FormSelect
                    control={control}
                    name="googleSettings.aiVersion"
                    label="AI Version (optional)"
                    placeholder="Default"
                    options={AI_VERSION_OPTIONS}
                />
                <FormAutocomplete
                    control={control}
                    name="googleSettings.endpoint"
                    label="Endpoint (optional)"
                    placeholder="https://generativelanguage.googleapis.com"
                    options={ENDPOINTS}
                />
                {isChat ? (
                    <PromptCacheField baseName="googleSettings" />
                ) : (
                    <>
                        <DimensionsField baseName="googleSettings" />
                        <EmbeddingsMaxConcurrentBatchesField baseName="googleSettings" />
                    </>
                )}
            </AdvancedFields>
        </>
    );
}
