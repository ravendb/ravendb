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
    EmbeddingsMaxConcurrentBatchesField,
    TemperatureField,
} from "@/components/ai-connection-string/provider-fields/shared-fields";
import { useAiModelOptions } from "@/components/ai-connection-string/use-ai-model-options";

const THINK_OPTIONS: FormSelectOption<ConnectionStringFormData["ollamaSettings"]["think"]>[] = [
    { value: "default", label: "Default" },
    { value: "enabled", label: "Enabled" },
    { value: "disabled", label: "Disabled" },
];

export function OllamaFields({ modelType }: { modelType: AiModelType }) {
    const { control, getValues } = useFormContext<ConnectionStringFormData>();
    const isModelLocked = useIsModelLocked();
    const isChat = modelType === "Chat";

    const settings = getValues("ollamaSettings");
    const hasAdvancedValues = Boolean(
        settings.think !== "default" || settings.isSetTemperature || settings.embeddingsMaxConcurrentBatches != null,
    );

    const uri = useWatch({ control, name: "ollamaSettings.uri" });
    const trimmedUri = uri.trim();
    const models = useAiModelOptions(
        trimmedUri
            ? {
                  connectorType: "Ollama",
                  ollamaSettings: { uri: trimmedUri },
              }
            : null,
    );

    return (
        <>
            <ExperimentalProviderAlert>
                Local models are experimental. Depending on the model you run, results may be significantly worse than
                with hosted providers.
            </ExperimentalProviderAlert>
            <FormInput control={control} name="ollamaSettings.uri" label="URI" placeholder="http://localhost:11434/" />
            <FormAutocomplete
                control={control}
                name="ollamaSettings.model"
                label="Model"
                placeholder="Select a model or enter a new one"
                options={models}
                disabled={isModelLocked}
                emptyMessage={trimmedUri ? "No models found." : "Provide a URI to load available models."}
            />
            <AdvancedFields defaultOpen={hasAdvancedValues}>
                {isChat ? (
                    <>
                        <FormSelect
                            control={control}
                            name="ollamaSettings.think"
                            label="Thinking mode"
                            options={THINK_OPTIONS}
                            description="Whether the model exposes its reasoning steps before answering."
                        />
                        <TemperatureField baseName="ollamaSettings" />
                    </>
                ) : (
                    <EmbeddingsMaxConcurrentBatchesField baseName="ollamaSettings" />
                )}
            </AdvancedFields>
        </>
    );
}
