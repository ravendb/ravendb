import { type FieldPath, useFormContext, useWatch } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import type { ConnectionStringFormData } from "@/components/ai-connection-string/ai-connection-string-utils";
import { FormCardSwitch } from "@/components/form/form-card-switch";

type PromptCacheProvider = Extract<
    ConnectionStringFormData["provider"],
    "openAiSettings" | "azureOpenAiSettings" | "googleSettings"
>;

type TemperatureProvider = Extract<
    ConnectionStringFormData["provider"],
    "openAiSettings" | "azureOpenAiSettings" | "ollamaSettings"
>;

type DimensionsProvider = Extract<
    ConnectionStringFormData["provider"],
    "openAiSettings" | "azureOpenAiSettings" | "googleSettings"
>;

export function PromptCacheField({ baseName }: { baseName: PromptCacheProvider }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const name = `${baseName}.enablePromptCache` satisfies FieldPath<ConnectionStringFormData>;

    return (
        <FormCardSwitch
            title="Enable prompt cache"
            description="Reuse cached prompt prefixes across turns in a conversation to reduce latency and cost."
            control={control}
            name={name}
        />
    );
}

export function TemperatureField({ baseName }: { baseName: TemperatureProvider }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const isSetName = `${baseName}.isSetTemperature` satisfies FieldPath<ConnectionStringFormData>;
    const temperatureName = `${baseName}.temperature` satisfies FieldPath<ConnectionStringFormData>;
    const isSetTemperature = useWatch({ control, name: isSetName });

    return (
        <div className="grid gap-3">
            <FormCardSwitch
                title="Set temperature"
                description="Off uses the model's default randomness."
                control={control}
                name={isSetName}
            />
            {isSetTemperature && (
                <FormInput
                    control={control}
                    name={temperatureName}
                    type="number"
                    step="0.1"
                    min={0}
                    max={2}
                    placeholder="e.g. 0.4"
                />
            )}
        </div>
    );
}

export function DimensionsField({ baseName }: { baseName: DimensionsProvider }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const name = `${baseName}.dimensions` satisfies FieldPath<ConnectionStringFormData>;

    return (
        <FormInput
            control={control}
            name={name}
            type="number"
            label="Dimensions (optional)"
            placeholder="Model default"
            description="The number of dimensions for the output embeddings."
        />
    );
}

export function EmbeddingsMaxConcurrentBatchesField({ baseName }: { baseName: ConnectionStringFormData["provider"] }) {
    const { control } = useFormContext<ConnectionStringFormData>();
    const name = `${baseName}.embeddingsMaxConcurrentBatches` satisfies FieldPath<ConnectionStringFormData>;

    return (
        <FormInput
            control={control}
            name={name}
            type="number"
            label="Max concurrent query batches (optional)"
            placeholder="Model default"
            description="Maximum number of query embedding batches processed concurrently."
        />
    );
}
