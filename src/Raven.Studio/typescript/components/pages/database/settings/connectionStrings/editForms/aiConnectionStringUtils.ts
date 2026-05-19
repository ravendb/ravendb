import { SelectOptionWithIcon } from "components/common/select/Select";
import { yupObjectSchema } from "components/utils/yupUtils";
import { AiConnection, ConnectionFormData } from "../connectionStringsTypes";
import { connectionStringsUtils } from "../connectionStringsUtils";
import * as yup from "yup";
import _ from "lodash";

type AiConnectionString = Raven.Client.Documents.Operations.AI.AiConnectionString;
type AiConnectorType = Raven.Client.Documents.Operations.AI.AiConnectorType;
type AiConnectionSetting = Exclude<
    keyof AiConnectionString,
    "Type" | "Identifier" | "ModelType" | "Name" | "UsedByTasks"
>;

const getConnectorType = (connection: AiConnectionString): AiConnectorType => {
    const mapping: Record<AiConnectionSetting, AiConnectorType> = {
        AzureOpenAiSettings: "AzureOpenAi",
        GoogleSettings: "Google",
        HuggingFaceSettings: "HuggingFace",
        OllamaSettings: "Ollama",
        EmbeddedSettings: "Embedded",
        OpenAiSettings: "OpenAi",
        MistralAiSettings: "MistralAi",
        VertexSettings: "Vertex",
    };

    for (const key of Object.keys(mapping) as AiConnectionSetting[]) {
        if (connection[key]) {
            return mapping[key];
        }
    }

    throw new Error("No connector type found. Please check the connection string.");
};

function mapAiConnectionStringToSettingsDto(connection: AiConnectionString): AiConnectionStringsSettings {
    const settings = [
        connection.AzureOpenAiSettings,
        connection.GoogleSettings,
        connection.HuggingFaceSettings,
        connection.OllamaSettings,
        connection.EmbeddedSettings,
        connection.OpenAiSettings,
        connection.MistralAiSettings,
        connection.VertexSettings,
    ].find(Boolean);

    if (!settings) {
        throw new Error("No settings found. Please check the connection string.");
    }

    return settings;
}

type FormData = ConnectionFormData<AiConnection>;

const chatConnectorTypes: FormData["connectorType"][] = [
    "azureOpenAiSettings",
    "googleSettings",
    "ollamaSettings",
    "openAiSettings",
];

function getConnectorOptions(modelType: FormData["modelType"]): SelectOptionWithIcon<FormData["connectorType"]>[] {
    // Alphabetical order beside of Embedded
    const allOptions: SelectOptionWithIcon<FormData["connectorType"]>[] = [
        { label: "Azure OpenAI", value: "azureOpenAiSettings", icon: "openai" },
        { label: "Google AI", value: "googleSettings", icon: "google-gemini" },
        { label: "Hugging Face", value: "huggingFaceSettings", icon: "huggingface" },
        { label: "Mistral AI", value: "mistralAiSettings", icon: "mistralai" },
        { label: "Ollama", value: "ollamaSettings", icon: "ollama" },
        { label: "OpenAI", value: "openAiSettings", icon: "openai" },
        { label: "Vertex AI", value: "vertexSettings", icon: "vertex-ai" },
        { label: "Embedded (bge-micro-v2)", value: "embeddedSettings", icon: "onnx" },
    ];

    if (modelType === "Chat") {
        return [...allOptions.filter((x) => chatConnectorTypes.includes(x.value))];
    }

    return allOptions;
}

const getTemperatureSchema = (connectorType: FormData["connectorType"]) =>
    yup
        .number()
        .nullable()
        .when(["$connectorType", "$modelType", "isSetTemperature"], {
            is: (
                currentConnectorType: FormData["connectorType"],
                modelType: FormData["modelType"],
                isSetTemperature: boolean
            ) => currentConnectorType === connectorType && modelType === "Chat" && isSetTemperature,
            then: (schema) => schema.min(0).max(2).required(),
        });

const getDimensionsSchema = (connectorType: FormData["connectorType"]) =>
    yup
        .number()
        .nullable()
        .when(["$connectorType", "$modelType"], {
            is: (currentConnectorType: FormData["connectorType"], modelType: FormData["modelType"]) =>
                currentConnectorType === connectorType && modelType === "TextEmbeddings",
            then: (schema) => schema.integer().positive(),
        });

const getEmbeddingsMaxConcurrentBatchesSchema = (connectorType: FormData["connectorType"]) =>
    yup
        .number()
        .nullable()
        .when(["$connectorType", "$modelType"], {
            is: (currentConnectorType: FormData["connectorType"], modelType: FormData["modelType"]) =>
                currentConnectorType === connectorType && modelType === "TextEmbeddings",
            then: (schema) => schema.integer().positive(),
        });

const schema = yupObjectSchema<FormData>({
    name: connectionStringsUtils.nameSchema,
    identifier: yup
        .string()
        .nullable()
        .test("is-identifier", "Only lowercase letters (a-z), numbers (0-9) and hyphens (-) are allowed.", (value) => {
            if (!value) {
                return true;
            }

            return /^[a-z0-9-]+$/.test(value);
        }),
    connectorType: yup.string<FormData["connectorType"]>().nullable().required(),
    modelType: yup.string<Raven.Client.Documents.Operations.AI.AiModelType>().nullable().required(),
    azureOpenAiSettings: yupObjectSchema<FormData["azureOpenAiSettings"]>({
        apiKey: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "azureOpenAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        endpoint: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "azureOpenAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "azureOpenAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        deploymentName: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "azureOpenAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        dimensions: getDimensionsSchema("azureOpenAiSettings"),
        embeddingsMaxConcurrentBatches: getEmbeddingsMaxConcurrentBatchesSchema("azureOpenAiSettings"),
        enablePromptCache: yup.boolean().nullable(),
        isSetTemperature: yup.boolean().nullable(),
        temperature: getTemperatureSchema("azureOpenAiSettings"),
    }),
    googleSettings: yupObjectSchema<FormData["googleSettings"]>({
        aiVersion: yup.string<Raven.Client.Documents.Operations.AI.GoogleAIVersion>().nullable(),
        apiKey: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "googleSettings",
                then: (schema) => schema.trim().required(),
            }),
        endpoint: yup.string().nullable(),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "googleSettings",
                then: (schema) => schema.trim().required(),
            }),
        dimensions: getDimensionsSchema("googleSettings"),
        embeddingsMaxConcurrentBatches: getEmbeddingsMaxConcurrentBatchesSchema("googleSettings"),
        enablePromptCache: yup.boolean().nullable(),
    }),
    huggingFaceSettings: yupObjectSchema<FormData["huggingFaceSettings"]>({
        apiKey: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "huggingFaceSettings",
                then: (schema) => schema.trim().required(),
            }),
        endpoint: yup.string().nullable(),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "huggingFaceSettings",
                then: (schema) => schema.trim().required(),
            }),
        embeddingsMaxConcurrentBatches: getEmbeddingsMaxConcurrentBatchesSchema("huggingFaceSettings"),
    }),
    ollamaSettings: yupObjectSchema<FormData["ollamaSettings"]>({
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "ollamaSettings",
                then: (schema) => schema.trim().required(),
            }),
        uri: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "ollamaSettings",
                then: (schema) => schema.trim().required(),
            }),
        embeddingsMaxConcurrentBatches: getEmbeddingsMaxConcurrentBatchesSchema("ollamaSettings"),
        think: yup.boolean().nullable(),
        isSetTemperature: yup.boolean().nullable(),
        temperature: getTemperatureSchema("ollamaSettings"),
    }),
    embeddedSettings: yupObjectSchema<FormData["embeddedSettings"]>({
        embeddingsMaxConcurrentBatches: getEmbeddingsMaxConcurrentBatchesSchema("embeddedSettings"),
    }),
    openAiSettings: yupObjectSchema<FormData["openAiSettings"]>({
        apiKey: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "openAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        endpoint: yup.string().nullable(),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "openAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        organizationId: yup.string().nullable(),
        projectId: yup.string().nullable(),
        dimensions: getDimensionsSchema("openAiSettings"),
        embeddingsMaxConcurrentBatches: getEmbeddingsMaxConcurrentBatchesSchema("openAiSettings"),
        enablePromptCache: yup.boolean().nullable(),
        isSetTemperature: yup.boolean().nullable(),
        temperature: getTemperatureSchema("openAiSettings"),
    }),
    vertexSettings: yupObjectSchema<FormData["vertexSettings"]>({
        aiVersion: yup.string<Raven.Client.Documents.Operations.AI.VertexAIVersion>().nullable(),
        googleCredentialsJson: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "vertexSettings",
                then: (schema) => schema.trim().required(),
            }),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "vertexSettings",
                then: (schema) => schema.trim().required(),
            }),
        location: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "vertexSettings",
                then: (schema) => schema.trim().required(),
            }),
        embeddingsMaxConcurrentBatches: getEmbeddingsMaxConcurrentBatchesSchema("vertexSettings"),
    }),
    mistralAiSettings: yupObjectSchema<FormData["mistralAiSettings"]>({
        apiKey: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "mistralaiAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        endpoint: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "mistralaiAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "mistralaiAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        embeddingsMaxConcurrentBatches: getEmbeddingsMaxConcurrentBatchesSchema("mistralAiSettings"),
    }),
    excludedDatabases: yup.array().of(yup.string()).optional(),
});

function getDefaultValues(initialConnection: AiConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            identifier: null,
            connectorType: null,
            modelType: initialConnection?.modelType ?? null,
            azureOpenAiSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                deploymentName: null,
                dimensions: null,
                embeddingsMaxConcurrentBatches: null,
                enablePromptCache: null,
                isSetTemperature: false,
                temperature: null,
            } satisfies Required<FormData["azureOpenAiSettings"]>,
            googleSettings: {
                aiVersion: null,
                apiKey: null,
                model: null,
                dimensions: null,
                endpoint: null,
                embeddingsMaxConcurrentBatches: null,
                enablePromptCache: null,
            } satisfies Required<FormData["googleSettings"]>,
            huggingFaceSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                embeddingsMaxConcurrentBatches: null,
            } satisfies Required<FormData["huggingFaceSettings"]>,
            ollamaSettings: {
                model: null,
                uri: null,
                embeddingsMaxConcurrentBatches: null,
                think: null,
                isSetTemperature: false,
                temperature: null,
            } satisfies Required<FormData["ollamaSettings"]>,
            embeddedSettings: {
                embeddingsMaxConcurrentBatches: null,
            } satisfies Required<FormData["embeddedSettings"]>,
            openAiSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                organizationId: null,
                projectId: null,
                dimensions: null,
                embeddingsMaxConcurrentBatches: null,
                enablePromptCache: null,
                isSetTemperature: false,
                temperature: null,
            } satisfies Required<FormData["openAiSettings"]>,
            vertexSettings: {
                aiVersion: null,
                googleCredentialsJson: null,
                location: null,
                model: null,
                embeddingsMaxConcurrentBatches: null,
            } satisfies Required<FormData["vertexSettings"]>,
            mistralAiSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                embeddingsMaxConcurrentBatches: null,
            } satisfies Required<FormData["mistralAiSettings"]>,
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}

export const aiConnectionStringUtils = {
    getConnectorType,
    mapAiConnectionStringToSettingsDto,
    getConnectorOptions,
    getDefaultValues,
    schema,
    chatConnectorTypes,
};
