import { useFormContext } from "react-hook-form";
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

const CHAT_MODELS = ["gpt-4o", "gpt-4o-mini", "gpt-4.1", "gpt-4.1-mini", "o3", "o4-mini"];
const EMBEDDINGS_MODELS = ["text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002"];

export function AzureOpenAiFields({ modelType }: { modelType: AiModelType }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const isChat = modelType === "Chat";

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
                placeholder="gpt-4o, gpt-4-turbo, …"
                options={isChat ? CHAT_MODELS : EMBEDDINGS_MODELS}
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
