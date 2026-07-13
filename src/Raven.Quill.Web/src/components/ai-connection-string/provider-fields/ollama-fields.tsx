import { useFormContext } from "react-hook-form";
import type { AiModelType } from "@/api/generated/server-api";
import { FormAutocomplete } from "@/components/form/form-autocomplete";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
import {
    EmbeddingsMaxConcurrentBatchesField,
    TemperatureField,
} from "@/components/ai-connection-string/provider-fields/shared-fields";

const CHAT_MODELS = ["llama3.1", "llama3.2", "qwen2.5", "mistral", "phi3", "gemma2"];
const EMBEDDINGS_MODELS = ["nomic-embed-text", "mxbai-embed-large", "all-minilm"];

const THINK_OPTIONS: FormSelectOption<ConnectionStringFormData["ollamaSettings"]["think"]>[] = [
    { value: "default", label: "Default" },
    { value: "enabled", label: "Enabled" },
    { value: "disabled", label: "Disabled" },
];

export function OllamaFields({ modelType }: { modelType: AiModelType }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const isChat = modelType === "Chat";

    return (
        <>
            <FormInput control={control} name="ollamaSettings.uri" label="URI" placeholder="http://localhost:11434/" />
            <FormAutocomplete
                control={control}
                name="ollamaSettings.model"
                label="Model"
                placeholder="llama3.1, mistral, …"
                options={isChat ? CHAT_MODELS : EMBEDDINGS_MODELS}
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
