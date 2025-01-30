import { Form, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSelect, FormSwitch } from "components/common/Form";
import { FormProvider, SubmitHandler, useForm, useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConnectionFormData, EditConnectionStringFormProps, AiConnection } from "../connectionStringsTypes";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import { yupObjectSchema } from "components/utils/yupUtils";
import { SelectOption } from "components/common/select/Select";
import RichAlert from "components/common/RichAlert";

type FormData = ConnectionFormData<AiConnection>;

export interface AiConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AiConnection;
}

export default function AiConnectionString({ initialConnection, isForNewConnection, onSave }: AiConnectionStringProps) {
    const form = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: (data, _, options) =>
            yupResolver(schema)(
                data,
                {
                    connectorType: data.connectorType,
                },
                options
            ),
    });

    const { control, handleSubmit } = form;

    // TODO kalczur
    // const { forCurrentDatabase } = useAppUrls();

    const formValues = useWatch({ control });

    const isUsedByAnyTask = !!initialConnection.usedByTasks?.length;

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "Ai",
            ...formData,
        } satisfies AiConnection);
    };

    return (
        <FormProvider {...form}>
            <Form id="connection-string-form" onSubmit={handleSubmit(handleSave)} className="vstack gap-3">
                <div className="mb-2">
                    <Label>Name</Label>
                    <FormInput
                        control={control}
                        name="name"
                        type="text"
                        placeholder="Enter a name for the connection string"
                        disabled={!isForNewConnection}
                        autoComplete="off"
                    />
                </div>
                <div className="mb-2">
                    <Label>
                        Identifier
                        <Icon icon="info" color="info" margin="ms-1" id="identifier-info" />
                    </Label>
                    <UncontrolledPopover target="identifier-info" trigger="hover" placement="top" className="bs5">
                        <PopoverBody>
                            A unique identifier used in document paths. If not specified, will be auto-generated from
                            the connection name.
                        </PopoverBody>
                    </UncontrolledPopover>
                    <FormInput
                        control={control}
                        name="identifier"
                        type="text"
                        placeholder="Enter identifier for the connection string"
                        disabled={isUsedByAnyTask}
                    />
                </div>
                <div className="mb-2">
                    <Label>Connector</Label>
                    <FormSelect
                        control={control}
                        name="connectorType"
                        placeholder="Select connector"
                        options={
                            [
                                { label: "Azure OpenAI", value: "azureOpenAiSettings" },
                                { label: "Google AI", value: "googleSettings" },
                                { label: "Hugging Face", value: "huggingFaceSettings" },
                                { label: "Ollama", value: "ollamaSettings" },
                                { label: "ONNX", value: "onnxSettings" },
                                { label: "OpenAI", value: "openAiSettings" },
                            ] satisfies SelectOption<FormData["connectorType"]>[]
                        }
                        isDisabled={isUsedByAnyTask}
                    />
                </div>

                {formValues.connectorType === "azureOpenAiSettings" && (
                    <AzureOpenAiSettings isUsedByAnyTask={isUsedByAnyTask} />
                )}
                {formValues.connectorType === "googleSettings" && <GoogleSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {formValues.connectorType === "huggingFaceSettings" && (
                    <HuggingFaceSettings isUsedByAnyTask={isUsedByAnyTask} />
                )}
                {formValues.connectorType === "ollamaSettings" && <OllamaSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {formValues.connectorType === "onnxSettings" && <OnnxSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {formValues.connectorType === "openAiSettings" && <OpenAiSettings isUsedByAnyTask={isUsedByAnyTask} />}

                {isUsedByAnyTask && (
                    <RichAlert variant="info">
                        Some options cannot be edited because this connection string is in use by a task. To modify
                        them, please create a new connection string.
                    </RichAlert>
                )}

                {/* TODO kalczur add urlProvider */}
                <ConnectionStringUsedByTasks tasks={initialConnection.usedByTasks} urlProvider={() => () => "#"} />
            </Form>
        </FormProvider>
    );
}

function AzureOpenAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

    return (
        <>
            <div className="mb-2">
                <Label>API Key</Label>
                <FormInput control={control} name="azureOpenAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>Endpoint</Label>
                <FormInput control={control} name="azureOpenAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>Model</Label>
                <FormInput control={control} name="azureOpenAiSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>Deployment Name</Label>
                <FormInput control={control} name="azureOpenAiSettings.deploymentName" type="text" />
            </div>
            <div className="mb-2">
                <Label>Dimensions</Label>
                <FormInput
                    control={control}
                    name="azureOpenAiSettings.dimensions"
                    type="number"
                    disabled={isUsedByAnyTask}
                />
            </div>
        </>
    );
}

function GoogleSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

    return (
        <>
            <div className="mb-2">
                <Label>AI Version</Label>
                <FormSelect
                    control={control}
                    name="googleSettings.aiVersion"
                    options={
                        [
                            { label: "V1", value: "V1" },
                            { label: "V1_Beta", value: "V1_Beta" },
                        ] satisfies SelectOption<FormData["googleSettings"]["aiVersion"]>[]
                    }
                    isDisabled={isUsedByAnyTask}
                />
            </div>
            <div className="mb-2">
                <Label>API Key</Label>
                <FormInput control={control} name="googleSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>Model</Label>
                <FormInput control={control} name="googleSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
        </>
    );
}

function HuggingFaceSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

    return (
        <>
            <div className="mb-2">
                <Label>API Key</Label>
                <FormInput control={control} name="huggingFaceSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>Endpoint</Label>
                <FormInput control={control} name="huggingFaceSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>Model</Label>
                <FormInput control={control} name="huggingFaceSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
        </>
    );
}

function OllamaSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

    return (
        <>
            <div className="mb-2">
                <Label>Model</Label>
                <FormInput control={control} name="ollamaSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>URI</Label>
                <FormInput control={control} name="ollamaSettings.uri" type="text" />
            </div>
        </>
    );
}

function OnnxSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

    return (
        <>
            <div className="mb-2">
                <FormSwitch
                    control={control}
                    name="onnxSettings.caseSensitive"
                    disabled={isUsedByAnyTask}
                    color="primary"
                >
                    Case Sensitive
                </FormSwitch>
            </div>
            <div className="mb-2">
                <FormSwitch
                    control={control}
                    name="onnxSettings.normalizeEmbeddings"
                    disabled={isUsedByAnyTask}
                    color="primary"
                >
                    Normalize Embeddings
                </FormSwitch>
            </div>
            <div className="mb-2">
                <Label>Maximum Tokens</Label>
                <FormInput
                    control={control}
                    name="onnxSettings.maximumTokens"
                    type="number"
                    disabled={isUsedByAnyTask}
                />
            </div>
            <div className="mb-2">
                <Label>CLS Token</Label>
                <FormInput control={control} name="onnxSettings.clsToken" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>Pad Token</Label>
                <FormInput control={control} name="onnxSettings.padToken" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>SEP Token</Label>
                <FormInput control={control} name="onnxSettings.sepToken" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>Unknown Token</Label>
                <FormInput control={control} name="onnxSettings.unknownToken" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>Pooling Mode</Label>
                <FormSelect
                    control={control}
                    name="onnxSettings.poolingMode"
                    options={
                        [
                            { label: "Max", value: "Max" },
                            { label: "Mean", value: "Mean" },
                            { label: "MeanSquareRootTokensLength", value: "MeanSquareRootTokensLength" },
                        ] satisfies SelectOption<FormData["onnxSettings"]["poolingMode"]>[]
                    }
                    isDisabled={isUsedByAnyTask}
                />
            </div>
            <div className="mb-2">
                <Label>Unicode Normalization</Label>
                <FormSelect
                    control={control}
                    name="onnxSettings.unicodeNormalization"
                    options={
                        [
                            { label: "FormC", value: "FormC" },
                            { label: "FormD", value: "FormD" },
                            { label: "FormKC", value: "FormKC" },
                            { label: "FormKD", value: "FormKD" },
                        ] satisfies SelectOption<FormData["onnxSettings"]["unicodeNormalization"]>[]
                    }
                    isDisabled={isUsedByAnyTask}
                />
            </div>
        </>
    );
}

function OpenAiSettings({ isUsedByAnyTask }: { isUsedByAnyTask: boolean }) {
    const { control } = useFormContext<FormData>();

    return (
        <>
            <div className="mb-2">
                <Label>API Key</Label>
                <FormInput control={control} name="openAiSettings.apiKey" type="password" passwordPreview />
            </div>
            <div className="mb-2">
                <Label>Endpoint</Label>
                <FormInput control={control} name="openAiSettings.endpoint" type="text" />
            </div>
            <div className="mb-2">
                <Label>Model</Label>
                <FormInput control={control} name="openAiSettings.model" type="text" disabled={isUsedByAnyTask} />
            </div>
            <div className="mb-2">
                <Label>Organization ID</Label>
                <FormInput control={control} name="openAiSettings.organizationId" type="text" />
            </div>
            <div className="mb-2">
                <Label>Project ID</Label>
                <FormInput control={control} name="openAiSettings.projectId" type="text" />
            </div>
        </>
    );
}

const schema = yupObjectSchema<FormData>({
    name: yup.string().nullable().required(),
    identifier: yup
        .string()
        .nullable()
        .test("is-identifier", "Only English letters, numbers and hyphens are allowed.", (value) => {
            if (!value) {
                return true;
            }

            return /^[a-zA-Z0-9-]+$/.test(value);
        }),
    connectorType: yup.string<FormData["connectorType"]>().nullable().required(),
    azureOpenAiSettings: yup.object({
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
        dimensions: yup.number().nullable().integer().positive(),
    }),
    googleSettings: yup.object({
        aiVersion: yup.string<Raven.Client.Documents.Operations.ETL.AI.GoogleAIVersion>().nullable(),
        apiKey: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "googleSettings",
                then: (schema) => schema.trim().required(),
            }),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "googleSettings",
                then: (schema) => schema.trim().required(),
            }),
    }),
    huggingFaceSettings: yup.object({
        apiKey: yup.string().nullable(),
        endpoint: yup.string().nullable(),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "huggingFaceSettings",
                then: (schema) => schema.trim().required(),
            }),
    }),
    ollamaSettings: yup.object({
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
    }),
    onnxSettings: yup.object({
        caseSensitive: yup.boolean().nullable(),
        clsToken: yup.string().nullable(),
        maximumTokens: yup.number().nullable(),
        normalizeEmbeddings: yup.boolean().nullable(),
        padToken: yup.string().nullable(),
        poolingMode: yup.string<Raven.Client.Documents.Operations.ETL.AI.OnnxEmbeddingPoolingMode>().nullable(),
        sepToken: yup.string().nullable(),
        unicodeNormalization: yup.string<System.Text.NormalizationForm>().nullable(),
        unknownToken: yup.string().nullable(),
    }),
    openAiSettings: yup.object({
        apiKey: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "openAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        endpoint: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "openAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        model: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "openAiSettings",
                then: (schema) => schema.trim().required(),
            }),
        organizationId: yup.string().nullable(),
        projectId: yup.string().nullable(),
    }),
});

function getDefaultValues(initialConnection: AiConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            identifier: null,
            connectorType: null,
            azureOpenAiSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                deploymentName: null,
                dimensions: null,
            },
            googleSettings: {
                aiVersion: null,
                apiKey: null,
                model: null,
            },
            huggingFaceSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
            },
            ollamaSettings: {
                model: null,
                uri: null,
            },
            onnxSettings: {
                caseSensitive: null,
                clsToken: null,
                maximumTokens: null,
                normalizeEmbeddings: null,
                padToken: null,
                poolingMode: null,
                sepToken: null,
                unicodeNormalization: null,
                unknownToken: null,
            },
            openAiSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                organizationId: null,
                projectId: null,
            },
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
