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

export function AzureOpenAiFields({ modelType }: { modelType: AiModelType }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const isChat = modelType === "Chat";

    const [apiKey, endpoint] = useWatch({
        control,
        name: ["azureOpenAiSettings.apiKey", "azureOpenAiSettings.endpoint"],
    });
    const trimmedApiKey = apiKey.trim();
    const trimmedEndpoint = endpoint.trim();
    const models = useAiModelOptions(
        trimmedApiKey && trimmedEndpoint
            ? {
                  connectorType: "AzureOpenAi",
                  azureOpenAiSettings: {
                      apiKey: trimmedApiKey,
                      endpoint: trimmedEndpoint,
                  },
              }
            : null,
    );

    return (
        <>
            <FormInput
                control={control}
                name="azureOpenAiSettings.apiKey"
                label="API Key"
                type="password"
                placeholder="..."
            />
            <FormInput
                control={control}
                name="azureOpenAiSettings.endpoint"
                label="Endpoint"
                placeholder="https://my-resource.openai.azure.com/"
            />
            <FormAutocomplete
                control={control}
                name="azureOpenAiSettings.model"
                label="Model"
                placeholder="Select a model or enter a new one"
                options={models}
                emptyMessage={
                    trimmedApiKey && trimmedEndpoint
                        ? "No models found."
                        : "Provide an API key and endpoint to load available models."
                }
            />
            <FormInput
                control={control}
                name="azureOpenAiSettings.deploymentName"
                label="Deployment Name"
                placeholder="my-deployment"
                description="The name of the deployed Azure OpenAI model."
            />
            {isChat ? (
                <>
                    <PromptCacheField baseName="azureOpenAiSettings" />
                    <TemperatureField baseName="azureOpenAiSettings" />
                </>
            ) : (
                <>
                    <DimensionsField baseName="azureOpenAiSettings" />
                    <EmbeddingsMaxConcurrentBatchesField baseName="azureOpenAiSettings" />
                </>
            )}
        </>
    );
}
