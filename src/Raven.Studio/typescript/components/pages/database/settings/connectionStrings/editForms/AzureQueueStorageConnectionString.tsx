import React, { useEffect } from "react";
import {
    ConnectionFormData,
    EditConnectionStringFormProps,
    AzureQueueStorageConnection,
} from "../connectionStringsTypes";
import { SelectOption } from "components/common/select/Select";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { yupObjectSchema } from "components/utils/yupUtils";
import { Control, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { useAppUrls } from "components/hooks/useAppUrls";
import { FormInput, FormSelect } from "components/common/Form";
import { Badge, Form, Label } from "reactstrap";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionStringUsedByTasks from "components/pages/database/settings/connectionStrings/editForms/shared/ConnectionStringUsedByTasks";
import { useServices } from "components/hooks/useServices";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { mapAzureQueueStorageConnectionStringSettingsToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";
import assertUnreachable from "components/utils/assertUnreachable";
import { Icon } from "components/common/Icon";

type FormData = ConnectionFormData<AzureQueueStorageConnection>;

export interface AzureQueueStorageConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AzureQueueStorageConnection;
}

export default function AzureQueueStorageConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: AzureQueueStorageConnectionStringProps) {
    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: (data, _, options) =>
            yupResolver(schema)(
                data,
                {
                    authType: data.authType,
                },
                options
            ),
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(`settings.${formValues.authType}`);
        if (!isValid) {
            return;
        }

        return tasksService.testAzureQueueStorageServerConnection(
            databaseName,
            mapAzureQueueStorageConnectionStringSettingsToDto(formValues)
        );
    });

    // Clear test result after changing auth type
    useEffect(() => {
        asyncTest.set(null);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [formValues.authType]);

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "AzureQueueStorage",
            ...formData,
        } satisfies AzureQueueStorageConnection);
    };

    return (
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
                <Label className="d-flex align-items-center gap-1">
                    Authentication{" "}
                    {asyncTest.result?.Success ? (
                        <Badge color="success" pill>
                            <Icon icon="check" />
                            Successfully connected
                        </Badge>
                    ) : asyncTest.result?.Error ? (
                        <Badge color="danger" pill>
                            <Icon icon="warning" />
                            Failed connection
                        </Badge>
                    ) : null}
                </Label>
                <FormSelect
                    name="authType"
                    control={control}
                    placeholder="Select an authentication option"
                    options={authenticationOptions}
                    isSearchable={false}
                />
            </div>
            <SelectedAuthFields control={control} authMethod={formValues.authType} />

            <div className="mb-2">
                <ButtonWithSpinner
                    color="secondary"
                    icon="rocket"
                    title="Test connection"
                    className="mb-2"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                    disabled={asyncTest.loading}
                >
                    Test connection
                </ButtonWithSpinner>
            </div>
            {asyncTest.result?.Error && (
                <div className="mb-2">
                    <ConnectionTestResult testResult={asyncTest.result} />
                </div>
            )}

            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editAzureQueueStorageEtl}
            />
        </Form>
    );
}

interface SelectedAuthFieldsProps {
    control: Control<FormData>;
    authMethod: AzureQueueStorageAuthenticationType;
}

function SelectedAuthFields({ control, authMethod }: SelectedAuthFieldsProps) {
    if (authMethod === "connectionString") {
        return (
            <div className="mb-2">
                <Label>Connection string</Label>
                <FormInput
                    control={control}
                    name="settings.connectionString.connectionStringValue"
                    type="textarea"
                    placeholder="Enter a connection string"
                />
            </div>
        );
    }

    if (authMethod === "entraId") {
        return (
            <div className="vstack gap-3">
                <div className="mb-2">
                    <Label>Client ID</Label>
                    <FormInput
                        control={control}
                        name="settings.entraId.clientId"
                        type="text"
                        placeholder="Enter a Client ID"
                    />
                </div>
                <div className="mb-2">
                    <Label>Client Secret</Label>
                    <FormInput
                        control={control}
                        name="settings.entraId.clientSecret"
                        type="password"
                        placeholder="Enter a Client Secret"
                        passwordPreview
                    />
                </div>
                <div className="mb-2">
                    <Label>Storage Account Name</Label>
                    <FormInput
                        control={control}
                        name="settings.entraId.storageAccountName"
                        type="text"
                        placeholder="Enter a Storage Account Name"
                    />
                </div>
                <div className="mb-2">
                    <Label>Tenant ID</Label>
                    <FormInput
                        control={control}
                        name="settings.entraId.tenantId"
                        type="text"
                        placeholder="Enter a Tenant ID"
                    />
                </div>
            </div>
        );
    }

    if (authMethod === "passwordless") {
        return (
            <div className="mb-2">
                <Label>Storage Account Name</Label>
                <FormInput
                    control={control}
                    name="settings.passwordless.storageAccountName"
                    type="text"
                    placeholder="Enter a Storage Account Name"
                />
            </div>
        );
    }

    assertUnreachable(authMethod);
}

const authenticationOptions: SelectOption<AzureQueueStorageAuthenticationType>[] = [
    {
        value: "connectionString",
        label: "Connection String",
    },
    {
        value: "entraId",
        label: "Entra ID",
    },
    {
        value: "passwordless",
        label: "Passwordless",
    },
];

function getStringRequiredSchema(authType: AzureQueueStorageAuthenticationType) {
    return yup
        .string()
        .nullable()
        .when("$authType", {
            is: authType,
            then: (schema) => schema.required(),
        });
}

const schema = yupObjectSchema<FormData>({
    name: yup.string().nullable().required(),
    authType: yup.string<AzureQueueStorageAuthenticationType>(),
    settings: yupObjectSchema<FormData["settings"]>({
        connectionString: yupObjectSchema<FormData["settings"]["connectionString"]>({
            connectionStringValue: getStringRequiredSchema("connectionString"),
        }),
        entraId: yupObjectSchema<FormData["settings"]["entraId"]>({
            clientId: getStringRequiredSchema("entraId"),
            clientSecret: getStringRequiredSchema("entraId"),
            storageAccountName: getStringRequiredSchema("entraId"),
            tenantId: getStringRequiredSchema("entraId"),
        }),
        passwordless: yupObjectSchema<FormData["settings"]["passwordless"]>({
            storageAccountName: getStringRequiredSchema("passwordless"),
        }),
    }),
});

function getDefaultValues(initialConnection: AzureQueueStorageConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            authType: "connectionString",
            settings: {
                connectionString: {
                    connectionStringValue: null,
                },
                entraId: {
                    clientId: null,
                    clientSecret: null,
                    storageAccountName: null,
                    tenantId: null,
                },
                passwordless: {
                    storageAccountName: null,
                },
            },
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
