import Button from "react-bootstrap/Button";
import { FormInput, FormLabel, FormSelect } from "components/common/Form";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConnectionFormData, EditConnectionStringFormProps, AiConnection } from "../connectionStringsTypes";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import { yupObjectSchema } from "components/utils/yupUtils";
import { SelectOptionWithIcon, SingleValueWithIcon } from "components/common/select/Select";
import RichAlert from "components/common/RichAlert";
import OptionalLabel from "components/common/OptionalLabel";
import AzureOpenAiSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/AzureOpenAiSettings";
import GoogleSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/GoogleSettings";
import HuggingFaceSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/HuggingFaceSettings";
import OllamaSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/OllamaSettings";
import OpenAiSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/OpenAiSettings";
import EmbeddedSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/EmbeddedSettings";
import MistralAiSettings from "./aiFields/MistralAiSettings";
import { useAppUrls } from "components/hooks/useAppUrls";
import TaskUtils from "components/utils/TaskUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { useAppSelector } from "components/store";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";
import { components, OptionProps } from "react-select";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import classNames from "classnames";
import Form from "react-bootstrap/Form";
import ModelTypeField from "./aiFields/ModelTypeField";

type FormData = ConnectionFormData<AiConnection>;

export interface AiConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AiConnection;
}

export default function AiConnectionString({ initialConnection, isForNewConnection, onSave }: AiConnectionStringProps) {
    const usedNames = useAppSelector(connectionStringSelectors.connections)["Ai"].map((x) => x.name);

    const form = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: (data, _, options) =>
            yupResolver(schema)(
                data,
                {
                    connectorType: data.connectorType,
                    isForNewConnection,
                    usedNames,
                } satisfies ConnectionStringsNameContext & { connectorType: FormData["connectorType"] },
                options
            ),
    });

    const { control, handleSubmit, setValue } = form;

    const { forCurrentDatabase } = useAppUrls();

    const formValues = useWatch({ control });
    const { connectorType, modelType } = formValues;

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
                    <FormLabel>Name</FormLabel>
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
                    <FormLabel>
                        Identifier <OptionalLabel />
                        <PopoverWithHoverWrapper
                            message="A unique identifier used in document paths. If not specified, it will be auto-generated
                                from the connection string name."
                        >
                            <Icon icon="info" color="info" margin="ms-1" id="identifier" />
                        </PopoverWithHoverWrapper>
                    </FormLabel>
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
                <ModelTypeField />
                <div className="mb-2">
                    <FormLabel>Connector</FormLabel>
                    <FormSelect
                        control={control}
                        name="connectorType"
                        placeholder={`Select connector${modelType == null ? " (select model type first)" : ""}`}
                        options={getConnectorOptions(modelType)}
                        isDisabled={isUsedByAnyTask || modelType == null}
                        components={{
                            Option: SettingsOptionComponent,
                            SingleValue: SingleValueWithIcon,
                        }}
                    />
                </div>
                {connectorType === "azureOpenAiSettings" && <AzureOpenAiSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "googleSettings" && <GoogleSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "huggingFaceSettings" && <HuggingFaceSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "ollamaSettings" && <OllamaSettings isUsedByAnyTask={isUsedByAnyTask} />}
                {connectorType === "embeddedSettings" && <EmbeddedSettings />}
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

export function SettingsOptionComponent(props: OptionProps<SelectOptionWithIcon>) {
    const { data } = props;

    const hasEmbeddingsGeneration = useAppSelector(licenseSelectors.statusValue("HasEmbeddingsGeneration"));

    const isDisabled = !hasEmbeddingsGeneration && data.value !== "embeddedSettings";

    return (
        <div className={classNames("cursor-pointer", { "pe-none": isDisabled })}>
            <components.Option {...props} isDisabled={isDisabled}>
                {data.icon && <Icon icon={data.icon} color={data.iconColor} />}
                {data.label}
                {isDisabled && <LicenseRestrictedBadge licenseRequired="Enterprise" />}
            </components.Option>
        </div>
    );
}

function getConnectorOptions(modelType: FormData["modelType"]): SelectOptionWithIcon<FormData["connectorType"]>[] {
    const allOptions: SelectOptionWithIcon<FormData["connectorType"]>[] = [
        { label: "Azure OpenAI", value: "azureOpenAiSettings", icon: "openai" },
        { label: "Google AI", value: "googleSettings", icon: "google-gemini" },
        { label: "Hugging Face", value: "huggingFaceSettings", icon: "huggingface" },
        { label: "Ollama", value: "ollamaSettings", icon: "ollama" },
        { label: "OpenAI", value: "openAiSettings", icon: "openai" },
        { label: "Mistral AI", value: "mistralAiSettings", icon: "mistralai" },
        { label: "Embedded (bge-micro-v2)", value: "embeddedSettings", icon: "onnx" },
    ];

    if (modelType === "Chat") {
        return [
            ...allOptions.filter(
                (x) => x.value === "ollamaSettings" || x.value === "openAiSettings" || x.value === "azureOpenAiSettings"
            ),
        ].reverse();
    }

    return allOptions;
}

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
        embeddingsMaxConcurrentBatches: yup.number().nullable().integer().positive(),
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
        dimensions: yup.number().nullable().integer().positive(),
        embeddingsMaxConcurrentBatches: yup.number().nullable().integer().positive(),
    }),
    huggingFaceSettings: yup.object({
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
        embeddingsMaxConcurrentBatches: yup.number().nullable().integer().positive(),
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
        embeddingsMaxConcurrentBatches: yup.number().nullable().integer().positive(),
    }),
    embeddedSettings: yup.object({
        embeddingsMaxConcurrentBatches: yup.number().nullable().integer().positive(),
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
        dimensions: yup.number().nullable().integer().positive(),
        embeddingsMaxConcurrentBatches: yup.number().nullable().integer().positive(),
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
        embeddingsMaxConcurrentBatches: yup.number().nullable().integer().positive(),
    }),
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
            },
            googleSettings: {
                aiVersion: null,
                apiKey: null,
                model: null,
                dimensions: null,
                embeddingsMaxConcurrentBatches: null,
            },
            huggingFaceSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                embeddingsMaxConcurrentBatches: null,
            },
            ollamaSettings: {
                model: null,
                uri: null,
                embeddingsMaxConcurrentBatches: null,
            },
            embeddedSettings: {
                embeddingsMaxConcurrentBatches: null,
            },
            openAiSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                organizationId: null,
                projectId: null,
                dimensions: null,
                embeddingsMaxConcurrentBatches: null,
            },
            mistralAiSettings: {
                apiKey: null,
                endpoint: null,
                model: null,
                embeddingsMaxConcurrentBatches: null,
            },
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
