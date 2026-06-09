import { z } from "zod";
import type { AiConnectionString, AiConnectorType, AiModelType } from "@/api/generated/server-api";
import type { FormSelectOption } from "@/components/form/form-select";

// The form keeps one settings object per provider and switches the active one with `provider`,
// mirroring how RavenDB's Studio edits AI connection strings. The discriminator value is the
// settings key so it maps straight onto the AiConnectionString DTO.
export const PROVIDER_KEYS = [
    "openAiSettings",
    "azureOpenAiSettings",
    "ollamaSettings",
    "googleSettings",
    "huggingFaceSettings",
    "mistralAiSettings",
    "vertexSettings",
    "embeddedSettings",
] as const;

export type ProviderKey = (typeof PROVIDER_KEYS)[number];

// Connectors RavenDB accepts for chat models. The rest are embeddings-only.
const CHAT_PROVIDER_KEYS: ProviderKey[] = ["openAiSettings", "azureOpenAiSettings", "ollamaSettings", "googleSettings"];

const PROVIDER_LABELS: Record<ProviderKey, string> = {
    azureOpenAiSettings: "Azure OpenAI",
    embeddedSettings: "Embedded (bge-micro-v2)",
    googleSettings: "Google AI",
    huggingFaceSettings: "Hugging Face",
    mistralAiSettings: "Mistral AI",
    ollamaSettings: "Ollama",
    openAiSettings: "OpenAI",
    vertexSettings: "Vertex AI",
};

// Alphabetical, with Embedded last, matching Studio's ordering.
const PROVIDER_ORDER: ProviderKey[] = [
    "azureOpenAiSettings",
    "googleSettings",
    "huggingFaceSettings",
    "mistralAiSettings",
    "ollamaSettings",
    "openAiSettings",
    "vertexSettings",
    "embeddedSettings",
];

export function getProviderOptions(modelType: AiModelType): FormSelectOption<ProviderKey>[] {
    const keys =
        modelType === "Chat" ? PROVIDER_ORDER.filter((key) => CHAT_PROVIDER_KEYS.includes(key)) : PROVIDER_ORDER;
    return keys.map((key) => ({ value: key, label: PROVIDER_LABELS[key] }));
}

const AI_VERSION_VALUES = ["", "V1", "V1_Beta"] as const;

const connectionStringObject = z.object({
    name: z.string().trim().min(1, "Name is required"),
    provider: z.enum(PROVIDER_KEYS),
    openAiSettings: z.object({
        apiKey: z.string(),
        model: z.string(),
        endpoint: z.string(),
        organizationId: z.string(),
        projectId: z.string(),
        dimensions: z.number().nullable(),
        embeddingsMaxConcurrentBatches: z.number().nullable(),
        enablePromptCache: z.boolean(),
        isSetTemperature: z.boolean(),
        temperature: z.number().nullable(),
    }),
    azureOpenAiSettings: z.object({
        apiKey: z.string(),
        endpoint: z.string(),
        model: z.string(),
        deploymentName: z.string(),
        dimensions: z.number().nullable(),
        embeddingsMaxConcurrentBatches: z.number().nullable(),
        enablePromptCache: z.boolean(),
        isSetTemperature: z.boolean(),
        temperature: z.number().nullable(),
    }),
    ollamaSettings: z.object({
        uri: z.string(),
        model: z.string(),
        think: z.enum(["default", "enabled", "disabled"]),
        embeddingsMaxConcurrentBatches: z.number().nullable(),
        isSetTemperature: z.boolean(),
        temperature: z.number().nullable(),
    }),
    googleSettings: z.object({
        aiVersion: z.enum(AI_VERSION_VALUES),
        apiKey: z.string(),
        endpoint: z.string(),
        model: z.string(),
        dimensions: z.number().nullable(),
        embeddingsMaxConcurrentBatches: z.number().nullable(),
        enablePromptCache: z.boolean(),
    }),
    huggingFaceSettings: z.object({
        apiKey: z.string(),
        endpoint: z.string(),
        model: z.string(),
        embeddingsMaxConcurrentBatches: z.number().nullable(),
    }),
    mistralAiSettings: z.object({
        apiKey: z.string(),
        endpoint: z.string(),
        model: z.string(),
        embeddingsMaxConcurrentBatches: z.number().nullable(),
    }),
    vertexSettings: z.object({
        aiVersion: z.enum(AI_VERSION_VALUES),
        googleCredentialsJson: z.string(),
        model: z.string(),
        location: z.string(),
        embeddingsMaxConcurrentBatches: z.number().nullable(),
    }),
    embeddedSettings: z.object({
        embeddingsMaxConcurrentBatches: z.number().nullable(),
    }),
});

export type ConnectionStringFormData = z.infer<typeof connectionStringObject>;

// Required text fields per provider (validated only for the active provider).
const REQUIRED_FIELDS: Record<ProviderKey, { field: string; message: string }[]> = {
    openAiSettings: [
        { field: "apiKey", message: "API key is required" },
        { field: "model", message: "Model is required" },
    ],
    azureOpenAiSettings: [
        { field: "apiKey", message: "API key is required" },
        { field: "endpoint", message: "Endpoint is required" },
        { field: "model", message: "Model is required" },
        { field: "deploymentName", message: "Deployment name is required" },
    ],
    ollamaSettings: [
        { field: "uri", message: "URI is required" },
        { field: "model", message: "Model is required" },
    ],
    googleSettings: [
        { field: "apiKey", message: "API key is required" },
        { field: "model", message: "Model is required" },
    ],
    huggingFaceSettings: [
        { field: "apiKey", message: "API key is required" },
        { field: "model", message: "Model is required" },
    ],
    mistralAiSettings: [
        { field: "apiKey", message: "API key is required" },
        { field: "endpoint", message: "Endpoint is required" },
        { field: "model", message: "Model is required" },
    ],
    vertexSettings: [
        { field: "googleCredentialsJson", message: "Google credentials JSON is required" },
        { field: "model", message: "Model is required" },
        { field: "location", message: "Location is required" },
    ],
    embeddedSettings: [],
};

// Providers that expose a temperature control (chat only).
const TEMPERATURE_PROVIDER_KEYS: ProviderKey[] = ["openAiSettings", "azureOpenAiSettings", "ollamaSettings"];

export function createConnectionStringSchema(modelType: AiModelType) {
    return connectionStringObject.superRefine((values, ctx) => {
        const provider = values.provider;
        const settings = values[provider] as Record<string, unknown>;

        for (const { field, message } of REQUIRED_FIELDS[provider]) {
            const value = settings[field];
            if (typeof value !== "string" || value.trim().length === 0) {
                ctx.addIssue({ code: "custom", path: [provider, field], message });
            }
        }

        if (modelType === "Chat" && TEMPERATURE_PROVIDER_KEYS.includes(provider)) {
            const isSetTemperature = settings.isSetTemperature === true;
            const temperature = settings.temperature;
            if (isSetTemperature && (typeof temperature !== "number" || temperature < 0 || temperature > 2)) {
                ctx.addIssue({
                    code: "custom",
                    path: [provider, "temperature"],
                    message: "Enter a value between 0 and 2",
                });
            }
        }
    });
}

export function getDefaultValues(): ConnectionStringFormData {
    return {
        name: "",
        provider: "openAiSettings",
        openAiSettings: {
            apiKey: "",
            model: "",
            endpoint: "",
            organizationId: "",
            projectId: "",
            dimensions: null,
            embeddingsMaxConcurrentBatches: null,
            enablePromptCache: true,
            isSetTemperature: false,
            temperature: null,
        },
        azureOpenAiSettings: {
            apiKey: "",
            endpoint: "",
            model: "",
            deploymentName: "",
            dimensions: null,
            embeddingsMaxConcurrentBatches: null,
            enablePromptCache: true,
            isSetTemperature: false,
            temperature: null,
        },
        ollamaSettings: {
            uri: "http://localhost:11434/",
            model: "",
            think: "default",
            embeddingsMaxConcurrentBatches: null,
            isSetTemperature: false,
            temperature: null,
        },
        googleSettings: {
            aiVersion: "",
            apiKey: "",
            endpoint: "",
            model: "",
            dimensions: null,
            embeddingsMaxConcurrentBatches: null,
            enablePromptCache: false,
        },
        huggingFaceSettings: {
            apiKey: "",
            endpoint: "",
            model: "",
            embeddingsMaxConcurrentBatches: null,
        },
        mistralAiSettings: {
            apiKey: "",
            endpoint: "",
            model: "",
            embeddingsMaxConcurrentBatches: null,
        },
        vertexSettings: {
            aiVersion: "",
            googleCredentialsJson: "",
            model: "",
            location: "",
            embeddingsMaxConcurrentBatches: null,
        },
        embeddedSettings: {
            embeddingsMaxConcurrentBatches: null,
        },
    };
}

function trimOrNull(value: string): string | null {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
}

function aiVersionOrNull(value: (typeof AI_VERSION_VALUES)[number]) {
    return value === "" ? null : value;
}

const THINK_VALUES: Record<ConnectionStringFormData["ollamaSettings"]["think"], boolean | null> = {
    default: null,
    enabled: true,
    disabled: false,
};

export function mapFormDataToDto(values: ConnectionStringFormData, modelType: AiModelType): AiConnectionString {
    const isChat = modelType === "Chat";
    const base = { name: values.name.trim(), modelType };

    switch (values.provider) {
        case "openAiSettings": {
            const settings = values.openAiSettings;
            return {
                ...base,
                openAiSettings: {
                    apiKey: settings.apiKey.trim(),
                    model: settings.model.trim(),
                    endpoint: trimOrNull(settings.endpoint),
                    organizationId: trimOrNull(settings.organizationId),
                    projectId: trimOrNull(settings.projectId),
                    ...(isChat
                        ? {
                              enablePromptCache: settings.enablePromptCache,
                              temperature: settings.isSetTemperature ? settings.temperature : null,
                          }
                        : {
                              dimensions: settings.dimensions,
                              embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches,
                          }),
                },
            };
        }
        case "azureOpenAiSettings": {
            const settings = values.azureOpenAiSettings;
            return {
                ...base,
                azureOpenAiSettings: {
                    apiKey: settings.apiKey.trim(),
                    endpoint: settings.endpoint.trim(),
                    model: settings.model.trim(),
                    deploymentName: settings.deploymentName.trim(),
                    ...(isChat
                        ? {
                              enablePromptCache: settings.enablePromptCache,
                              temperature: settings.isSetTemperature ? settings.temperature : null,
                          }
                        : {
                              dimensions: settings.dimensions,
                              embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches,
                          }),
                },
            };
        }
        case "ollamaSettings": {
            const settings = values.ollamaSettings;
            return {
                ...base,
                ollamaSettings: {
                    uri: settings.uri.trim(),
                    model: settings.model.trim(),
                    ...(isChat
                        ? {
                              think: THINK_VALUES[settings.think],
                              temperature: settings.isSetTemperature ? settings.temperature : null,
                          }
                        : { embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches }),
                },
            };
        }
        case "googleSettings": {
            const settings = values.googleSettings;
            return {
                ...base,
                googleSettings: {
                    aiVersion: aiVersionOrNull(settings.aiVersion),
                    apiKey: settings.apiKey.trim(),
                    endpoint: trimOrNull(settings.endpoint),
                    model: settings.model.trim(),
                    ...(isChat
                        ? { enablePromptCache: settings.enablePromptCache }
                        : {
                              dimensions: settings.dimensions,
                              embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches,
                          }),
                },
            };
        }
        case "huggingFaceSettings": {
            const settings = values.huggingFaceSettings;
            return {
                ...base,
                huggingFaceSettings: {
                    apiKey: settings.apiKey.trim(),
                    endpoint: trimOrNull(settings.endpoint),
                    model: settings.model.trim(),
                    embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches,
                },
            };
        }
        case "mistralAiSettings": {
            const settings = values.mistralAiSettings;
            return {
                ...base,
                mistralAiSettings: {
                    apiKey: settings.apiKey.trim(),
                    endpoint: settings.endpoint.trim(),
                    model: settings.model.trim(),
                    embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches,
                },
            };
        }
        case "vertexSettings": {
            const settings = values.vertexSettings;
            return {
                ...base,
                vertexSettings: {
                    aiVersion: aiVersionOrNull(settings.aiVersion),
                    googleCredentialsJson: settings.googleCredentialsJson.trim(),
                    model: settings.model.trim(),
                    location: settings.location.trim(),
                    embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches,
                },
            };
        }
        case "embeddedSettings": {
            const settings = values.embeddedSettings;
            return {
                ...base,
                embeddedSettings: {
                    embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches,
                },
            };
        }
    }
}

export const CONNECTOR_TYPE_LABELS: Record<AiConnectorType, string> = {
    None: "—",
    OpenAi: "OpenAI",
    AzureOpenAi: "Azure OpenAI",
    Ollama: "Ollama",
    Embedded: "Embedded (bge-micro-v2)",
    Google: "Google AI",
    HuggingFace: "Hugging Face",
    MistralAi: "Mistral AI",
    Vertex: "Vertex AI",
};

export const MODEL_TYPE_LABELS: Record<AiModelType, string> = {
    Chat: "Chat",
    TextEmbeddings: "Text embeddings",
};

export function mapDtoToFormData(dto: AiConnectionString): ConnectionStringFormData {
    const base: ConnectionStringFormData = { ...getDefaultValues(), name: dto.name ?? "" };
    const text = (value: string | null | undefined) => value ?? "";

    if (dto.openAiSettings) {
        const settings = dto.openAiSettings;
        return {
            ...base,
            provider: "openAiSettings",
            openAiSettings: {
                apiKey: text(settings.apiKey),
                model: text(settings.model),
                endpoint: text(settings.endpoint),
                organizationId: text(settings.organizationId),
                projectId: text(settings.projectId),
                dimensions: settings.dimensions ?? null,
                embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches ?? null,
                enablePromptCache: settings.enablePromptCache ?? true,
                isSetTemperature: settings.temperature != null,
                temperature: settings.temperature ?? null,
            },
        };
    }

    if (dto.azureOpenAiSettings) {
        const settings = dto.azureOpenAiSettings;
        return {
            ...base,
            provider: "azureOpenAiSettings",
            azureOpenAiSettings: {
                apiKey: text(settings.apiKey),
                endpoint: text(settings.endpoint),
                model: text(settings.model),
                deploymentName: text(settings.deploymentName),
                dimensions: settings.dimensions ?? null,
                embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches ?? null,
                enablePromptCache: settings.enablePromptCache ?? true,
                isSetTemperature: settings.temperature != null,
                temperature: settings.temperature ?? null,
            },
        };
    }

    if (dto.ollamaSettings) {
        const settings = dto.ollamaSettings;
        return {
            ...base,
            provider: "ollamaSettings",
            ollamaSettings: {
                uri: text(settings.uri),
                model: text(settings.model),
                think: settings.think === true ? "enabled" : settings.think === false ? "disabled" : "default",
                embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches ?? null,
                isSetTemperature: settings.temperature != null,
                temperature: settings.temperature ?? null,
            },
        };
    }

    if (dto.googleSettings) {
        const settings = dto.googleSettings;
        return {
            ...base,
            provider: "googleSettings",
            googleSettings: {
                aiVersion: settings.aiVersion ?? "",
                apiKey: text(settings.apiKey),
                endpoint: text(settings.endpoint),
                model: text(settings.model),
                dimensions: settings.dimensions ?? null,
                embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches ?? null,
                enablePromptCache: settings.enablePromptCache ?? false,
            },
        };
    }

    if (dto.huggingFaceSettings) {
        const settings = dto.huggingFaceSettings;
        return {
            ...base,
            provider: "huggingFaceSettings",
            huggingFaceSettings: {
                apiKey: text(settings.apiKey),
                endpoint: text(settings.endpoint),
                model: text(settings.model),
                embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches ?? null,
            },
        };
    }

    if (dto.mistralAiSettings) {
        const settings = dto.mistralAiSettings;
        return {
            ...base,
            provider: "mistralAiSettings",
            mistralAiSettings: {
                apiKey: text(settings.apiKey),
                endpoint: text(settings.endpoint),
                model: text(settings.model),
                embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches ?? null,
            },
        };
    }

    if (dto.vertexSettings) {
        const settings = dto.vertexSettings;
        return {
            ...base,
            provider: "vertexSettings",
            vertexSettings: {
                aiVersion: settings.aiVersion ?? "",
                googleCredentialsJson: text(settings.googleCredentialsJson),
                model: text(settings.model),
                location: text(settings.location),
                embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches ?? null,
            },
        };
    }

    if (dto.embeddedSettings) {
        const settings = dto.embeddedSettings;
        return {
            ...base,
            provider: "embeddedSettings",
            embeddedSettings: {
                embeddingsMaxConcurrentBatches: settings.embeddingsMaxConcurrentBatches ?? null,
            },
        };
    }

    return base;
}
