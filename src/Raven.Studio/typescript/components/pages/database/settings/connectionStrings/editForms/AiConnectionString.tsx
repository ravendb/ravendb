import { Form, Label } from "reactstrap";
import Button from "react-bootstrap/Button";
import { FormInput, FormSelect } from "components/common/Form";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConnectionFormData, EditConnectionStringFormProps, AiConnection } from "../connectionStringsTypes";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import { yupObjectSchema } from "components/utils/yupUtils";
import { OptionWithIcon, SelectOptionWithIcon, SingleValueWithIcon } from "components/common/select/Select";
import RichAlert from "components/common/RichAlert";
import OptionalLabel from "components/common/OptionalLabel";
import AzureOpenAiSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/AzureOpenAiSettings";
import GoogleSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/GoogleSettings";
import HuggingFaceSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/HuggingFaceSettings";
import OllamaSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/OllamaSettings";
import OpenAiSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/OpenAiSettings";
import MistralAiSettings from "./aiFields/MistralAiSettings";
import { useAppUrls } from "components/hooks/useAppUrls";
import TaskUtils from "components/utils/TaskUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";

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

    const { control, handleSubmit, setValue } = form;

    const { forCurrentDatabase } = useAppUrls();

    const formValues = useWatch({ control });
    const { connectorType } = formValues;

    const handleGenerateIdentifier = () => {
        setValue("identifier", TaskUtils.getGeneratedIdentifier(formValues.name));
    };

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
                        onBlur={() => {
                            if (!formValues.identifier) {
                                handleGenerateIdentifier();
                            }
                        }}
                    />
                </div>
                <div className="mb-2">
                    <Label>
                        Identifier <OptionalLabel />
                        <PopoverWithHoverWrapper
                            message="A unique identifier used in document paths. If not specified, it will be auto-generated
                                from the connection string name."
                        >
                            <Icon icon="info" color="info" margin="ms-1" id="identifier" />
                        </PopoverWithHoverWrapper>
                    </Label>
                    <FormInput
                        control={control}
                        name="identifier"
                        type="text"
                        placeholder="Enter an identifier for the connection string"
                        disabled={isUsedByAnyTask}
                        addon={
                            <Button
                                variant="link"
                                className="text-reset px-0"
                                onClick={handleGenerateIdentifier}
                                title="Click to generate the identifier from the connection string name"
                            >
                                <Icon icon="refresh" />
                                Regenerate
                            </Button>
                        }
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
                                { label: "Azure OpenAI", value: "azureOpenAiSettings", icon: "openai" },
                                { label: "Google AI", value: "googleSettings", icon: "google-gemini" },
                                { label: "Hugging Face", value: "huggingFaceSettings", icon: "huggingface" },
                                { label: "Ollama", value: "ollamaSettings", icon: "ollama" },
                                { label: "OpenAI", value: "openAiSettings", icon: "openai" },
                                { label: "Mistral AI", value: "mistralAiSettings", icon: "mistralai" },
                                { label: "Embedded (bge-micro-v2)", value: "embeddedSettings", icon: "onnx" },
                            ] satisfies SelectOptionWithIcon<FormData["connectorType"]>[]
                        }
                        isDisabled={isUsedByAnyTask}
                        components={{
                            Option: OptionWithIcon,
                            SingleValue: SingleValueWithIcon,
                        }}
                    />
                </div>

                {connectorType === "azureOpenAiSettings" && <AzureOpenAiSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "googleSettings" && <GoogleSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "huggingFaceSettings" && <HuggingFaceSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "ollamaSettings" && <OllamaSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "openAiSettings" && <OpenAiSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "mistralAiSettings" && <MistralAiSettings isUsedByAnyTask={isUsedByAnyTask} />}

                {isUsedByAnyTask && (
                    <RichAlert variant="info">
                        Some options cannot be edited because this connection string is in use by a task.
                        <br />
                        To modify them, please create a new connection string.
                    </RichAlert>
                )}

                <ConnectionStringUsedByTasks
                    tasks={initialConnection.usedByTasks}
                    urlProvider={forCurrentDatabase.editEmbeddingsGeneration}
                />
            </Form>
        </FormProvider>
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
        aiVersion: yup.string<Raven.Client.Documents.Operations.AI.GoogleAIVersion>().nullable(),
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
        endpoint: yup
            .string()
            .nullable()
            .when("$connectorType", {
                is: "huggingFaceSettings",
                then: (schema) => schema.trim().required(),
            }),
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
    onnxSettings: yup.object({}),
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
    mistralAiSettings: yup.object({
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
            onnxSettings: {},
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
