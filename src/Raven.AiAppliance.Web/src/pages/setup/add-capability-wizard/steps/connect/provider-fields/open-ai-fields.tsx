import { useFormContext } from "react-hook-form";
import type { AiModelType } from "@/api/generated/server-api";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import type { ConnectionStringFormData } from "@/pages/setup/add-capability-wizard/steps/connect/ai-connection-string-utils";
import {
    DimensionsField,
    EmbeddingsMaxConcurrentBatchesField,
    PromptCacheField,
    TemperatureField,
} from "@/pages/setup/add-capability-wizard/steps/connect/provider-fields/shared-fields";

// TODO get models from EP (not implemented yet)
const CHAT_MODELS = ["gpt-4o", "gpt-4o-mini", "gpt-4.1", "gpt-4.1-mini", "o3", "o4-mini"];
const EMBEDDINGS_MODELS = ["text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002"];
const ENDPOINTS = ["https://api.openai.com/v1/"];

export function OpenAiFields({ modelType }: { modelType: AiModelType }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const isChat = modelType === "Chat";

    return (
        <>
            <FormInput
                control={control}
                name="openAiSettings.apiKey"
                label="API Key"
                type="password"
                placeholder="sk-..."
            />
            <FormAutocomplete
                control={control}
                name="openAiSettings.model"
                label="Model"
                placeholder="gpt-4o, gpt-4-turbo, …"
                options={isChat ? CHAT_MODELS : EMBEDDINGS_MODELS}
            />
            <FormAutocomplete
                control={control}
                name="openAiSettings.endpoint"
                label="Endpoint (optional)"
                placeholder="https://api.openai.com/v1/"
                options={ENDPOINTS}
                description="Override for OpenAI-compatible providers."
            />
            <FormInput
                control={control}
                name="openAiSettings.organizationId"
                label="Organization ID (optional)"
                placeholder="org-..."
                description="Sets the OpenAI-Organization request header."
            />
            <FormInput
                control={control}
                name="openAiSettings.projectId"
                label="Project ID (optional)"
                placeholder="proj_..."
                description="Sets the OpenAI-Project request header."
            />
            {isChat ? (
                <>
                    <PromptCacheField baseName="openAiSettings" />
                    <TemperatureField baseName="openAiSettings" />
                </>
            ) : (
                <>
                    <DimensionsField baseName="openAiSettings" />
                    <EmbeddingsMaxConcurrentBatchesField baseName="openAiSettings" />
                </>
            )}
        </>
    );
}
