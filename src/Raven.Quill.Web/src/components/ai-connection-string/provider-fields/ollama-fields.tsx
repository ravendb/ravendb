import { useFormContext, useWatch } from "react-hook-form";
import type { AiModelType } from "@/api/generated/server-api";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
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
    const { control } = useFormContext<ConnectionStringFormData>();
    const isChat = modelType === "Chat";

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
            <FormInput control={control} name="ollamaSettings.uri" label="URI" placeholder="http://localhost:11434/" />
            <FormAutocomplete
                control={control}
                name="ollamaSettings.model"
                label="Model"
                placeholder="Select a model or enter a new one"
                options={models}
                emptyMessage={trimmedUri ? "No models found." : "Provide a URI to load available models."}
            />
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
        </>
    );
}
