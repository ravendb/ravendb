import { useFormContext, useWatch } from "react-hook-form";
import type { AiModelType } from "@/api/generated/server-api";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
import {
    DimensionsField,
    EmbeddingsMaxConcurrentBatchesField,
    PromptCacheField,
    TemperatureField,
} from "@/components/ai-connection-string/provider-fields/shared-fields";
import { useAiModelOptions } from "@/components/ai-connection-string/use-ai-model-options";

const ENDPOINTS = ["https://api.openai.com/v1/"];

export function OpenAiFields({ modelType }: { modelType: AiModelType }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const isChat = modelType === "Chat";

    const [apiKey, endpoint, organizationId, projectId] = useWatch({
        control,
        name: [
            "openAiSettings.apiKey",
            "openAiSettings.endpoint",
            "openAiSettings.organizationId",
            "openAiSettings.projectId",
        ],
    });
    const trimmedApiKey = apiKey.trim();
    const models = useAiModelOptions(
        trimmedApiKey
            ? {
                  connectorType: "OpenAi",
                  openAiSettings: {
                      apiKey: trimmedApiKey,
                      endpoint: endpoint.trim(),
                      organizationId: organizationId.trim(),
                      projectId: projectId.trim(),
                  },
              }
            : null,
    );

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
                placeholder="Select a model or enter a new one"
                options={models}
                emptyMessage={trimmedApiKey ? "No models found." : "Provide an API key to load available models."}
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
