import Button from "react-bootstrap/Button";
import { FormInput, FormLabel, FormSelect } from "components/common/Form";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConnectionFormData, EditConnectionStringFormProps, AiConnection } from "../connectionStringsTypes";
import { yupResolver } from "@hookform/resolvers/yup";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ExcludedDatabasesFormSelect from "./shared/ExcludedDatabasesFormSelect";
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
import VertexSettings from "components/pages/database/settings/connectionStrings/editForms/aiFields/VertexSettings";
import TaskUtils from "components/utils/TaskUtils";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { useAppSelector } from "components/store";
import { ConnectionStringsNameContext } from "../connectionStringsUtils";
import { components, OptionProps } from "react-select";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import classNames from "classnames";
import Form from "react-bootstrap/Form";
import ModelTypeField from "./aiFields/ModelTypeField";
import { withNestedSubmit } from "components/utils/common";
import { useEffect } from "react";
import { aiConnectionStringUtils } from "./aiConnectionStringUtils";

type FormData = ConnectionFormData<AiConnection>;

type FormSchemaContext = ConnectionStringsNameContext & {
    connectorType: FormData["connectorType"];
    modelType: FormData["modelType"];
};

export interface AiConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AiConnection;
}

export default function AiConnectionString({ initialConnection, isForNewConnection, onSave }: AiConnectionStringProps) {
    const usedNames = useAppSelector(connectionStringSelectors.connections)["Ai"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const form = useForm<FormData>({
        mode: "all",
        defaultValues: aiConnectionStringUtils.getDefaultValues(initialConnection, isForNewConnection),
        resolver: (data, _, options) =>
            yupResolver(aiConnectionStringUtils.schema)(
                data,
                {
                    connectorType: data.connectorType,
                    isForNewConnection,
                    usedNames,
                    modelType: data.modelType,
                } satisfies FormSchemaContext,
                options
            ),
    });

    const { control, handleSubmit, setValue, watch } = form;

    const formValues = useWatch({ control });
    const { connectorType, modelType } = formValues;

    // Reset connector when model type does not match it
    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if (
                name === "modelType" &&
                values.modelType === "Chat" &&
                values.connectorType != null &&
                !aiConnectionStringUtils.chatConnectorTypes.includes(values.connectorType)
            ) {
                setValue("connectorType", null);
            }
        });

        return () => unsubscribe();
    }, [setValue, watch]);

    const handleGenerateIdentifier = () => {
        setValue("identifier", TaskUtils.getGeneratedIdentifier(formValues.name));
    };

    const isUsedByAnyTask = !!initialConnection.usedBy?.length;

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "Ai",
            ...formData,
        } satisfies AiConnection);
    };

    return (
        <FormProvider {...form}>
            <Form
                id="connection-string-form"
                onSubmit={withNestedSubmit(handleSubmit(handleSave))}
                className="vstack gap-3"
            >
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
                <ModelTypeField initialModelType={initialConnection.modelType} />
                <div className="mb-2">
                    <FormLabel>Connector</FormLabel>
                    <FormSelect
                        control={control}
                        name="connectorType"
                        placeholder={`Select connector${modelType == null ? " (select model type first)" : ""}`}
                        options={aiConnectionStringUtils.getConnectorOptions(modelType)}
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
                {connectorType === "vertexSettings" && (
                    <VertexSettings isUsedByAnyTask={isUsedByAnyTask} isForNewConnection={isForNewConnection} />
                )}

                {isUsedByAnyTask && (
                    <RichAlert variant="info">
                        Some options cannot be edited because this connection string is in use by a task.
                        <br />
                        To modify them, please create a new connection string.
                    </RichAlert>
                )}

                <ConnectionStringUsedByTasks tasks={initialConnection.usedBy} connectionType={initialConnection.type} />
                {isServerWide && <ExcludedDatabasesFormSelect control={control} name="excludedDatabases" />}
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
