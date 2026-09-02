import { useFormContext, useWatch } from "react-hook-form";
import type { AiModelType } from "@/api/generated/server-api";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
import { useIsModelLocked } from "@/components/ai-connection-string/model-lock-context";
import { AdvancedFields } from "@/components/ai-connection-string/provider-fields/advanced-fields";
import { ExperimentalProviderAlert } from "@/components/ai-connection-string/provider-fields/experimental-provider-alert";
import {
    DimensionsField,
    EmbeddingsMaxConcurrentBatchesField,
    PromptCacheField,
    TemperatureField,
} from "@/components/ai-connection-string/provider-fields/shared-fields";
import { useAiModelOptions } from "@/components/ai-connection-string/use-ai-model-options";

const ENDPOINTS = ["https://api.openai.com/v1/"];

const REASONING_EFFORT_OPTIONS: FormSelectOption<ConnectionStringFormData["openAiSettings"]["reasoningEffort"]>[] = [
    { value: "default", label: "Default" },
    { value: "Minimal", label: "Minimal" },
    { value: "Low", label: "Low" },
    { value: "Medium", label: "Medium" },
    { value: "High", label: "High" },
];

export function OpenAiFields({ modelType }: { modelType: AiModelType }) {
    const { control, getValues } = useFormContext<ConnectionStringFormData>();
    const isModelLocked = useIsModelLocked();
    const isChat = modelType === "Chat";

    const settings = getValues("openAiSettings");
    const hasAdvancedValues = Boolean(
        settings.endpoint ||
        settings.organizationId ||
        settings.projectId ||
        settings.reasoningEffort !== "default" ||
        settings.isSetTemperature ||
        settings.dimensions != null ||
        settings.embeddingsMaxConcurrentBatches != null,
    );

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
    const trimmedEndpoint = endpoint.trim();
    const isCustomEndpoint = trimmedEndpoint !== "" && !trimmedEndpoint.startsWith("https://api.openai.com");
    const models = useAiModelOptions(
        trimmedApiKey
            ? {
                  connectorType: "OpenAi",
                  // Blank optional fields are omitted: an empty organization/project id is not
                  // the same as none to the upstream provider.
                  openAiSettings: {
                      apiKey: trimmedApiKey,
                      endpoint: endpoint.trim() || undefined,
                      organizationId: organizationId.trim() || undefined,
                      projectId: projectId.trim() || undefined,
                  },
              }
            : null,
    );

    return (
        <>
            {isCustomEndpoint && (
                <ExperimentalProviderAlert>
                    Using a custom endpoint. Self-hosted OpenAI-compatible servers such as vLLM are experimental and may
                    yield significantly worse results than OpenAI's hosted models.
                </ExperimentalProviderAlert>
            )}
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
                disabled={isModelLocked}
                emptyMessage={trimmedApiKey ? "No models found." : "Provide an API key to load available models."}
            />
            <AdvancedFields defaultOpen={hasAdvancedValues}>
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
                        <FormSelect
                            control={control}
                            name="openAiSettings.reasoningEffort"
                            label="Reasoning effort"
                            options={REASONING_EFFORT_OPTIONS}
                            description="How much the model reasons before answering. Applies to reasoning models only."
                        />
                        <PromptCacheField baseName="openAiSettings" />
                        <TemperatureField baseName="openAiSettings" />
                    </>
                ) : (
                    <>
                        <DimensionsField baseName="openAiSettings" />
                        <EmbeddingsMaxConcurrentBatchesField baseName="openAiSettings" />
                    </>
                )}
            </AdvancedFields>
        </>
    );
}
