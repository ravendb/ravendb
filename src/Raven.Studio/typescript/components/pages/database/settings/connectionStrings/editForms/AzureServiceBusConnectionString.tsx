import React, { useEffect } from "react";
import {
    AzureServiceBusConnection,
    ConnectionFormData,
    EditConnectionStringFormProps,
} from "../connectionStringsTypes";
import { SelectOption } from "components/common/select/Select";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { yupObjectSchema } from "components/utils/yupUtils";
import { Control, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { FormInput, FormLabel, FormSelect } from "components/common/Form";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionStringUsedByTasks from "components/pages/database/settings/connectionStrings/editForms/shared/ConnectionStringUsedByTasks";
import ExcludedDatabasesFormSelect from "./shared/ExcludedDatabasesFormSelect";
import { useServices } from "components/hooks/useServices";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { mapAzureServiceBusConnectionStringSettingsToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";
import assertUnreachable from "components/utils/assertUnreachable";
import { Icon } from "components/common/Icon";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";

type FormData = ConnectionFormData<AzureServiceBusConnection>;

export interface AzureServiceBusConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AzureServiceBusConnection;
}

export default function AzureServiceBusConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: AzureServiceBusConnectionStringProps) {
    const usedNames = useAppSelector(connectionStringSelectors.connections)["AzureServiceBus"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: (data, _, options) =>
            yupResolver(schema)(
                data,
                {
                    authType: data.authType,
                    isForNewConnection,
                    usedNames,
                } satisfies ConnectionStringsNameContext & { authType: FormData["authType"] },
                options
            ),
    });

    const formValues = useWatch({ control });
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(`settings.${formValues.authType}`);
        if (!isValid) {
            return;
        }

        return tasksService.testAzureServiceBusServerConnection(
            databaseName,
            mapAzureServiceBusConnectionStringSettingsToDto(formValues)
        );
    });

    useEffect(() => {
        asyncTest.set(null);
    }, [formValues.authType]);

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "AzureServiceBus",
            ...formData,
        } satisfies AzureServiceBusConnection);
    };

    return (
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
                />
            </div>
            <div className="mb-2">
                <FormLabel className="d-flex align-items-center gap-1">
                    Authentication{" "}
                    {asyncTest.result?.Success ? (
                        <Badge bg="success" pill>
                            <Icon icon="check" />
                            Successfully connected
                        </Badge>
                    ) : asyncTest.result?.Error ? (
                        <Badge bg="danger" pill>
                            <Icon icon="warning" />
                            Failed connection
                        </Badge>
                    ) : null}
                </FormLabel>
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
                    variant="secondary"
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

            <ConnectionStringUsedByTasks tasks={initialConnection.usedBy} connectionType={initialConnection.type} />
            {isServerWide && <ExcludedDatabasesFormSelect control={control} name="excludedDatabases" usedBy={initialConnection.usedBy} />}
        </Form>
    );
}

interface SelectedAuthFieldsProps {
    control: Control<FormData>;
    authMethod: AzureServiceBusAuthenticationType;
}

function SelectedAuthFields({ control, authMethod }: SelectedAuthFieldsProps) {
    if (authMethod === "connectionString") {
        return (
            <div className="mb-2">
                <FormLabel>Connection string</FormLabel>
                <FormInput
                    control={control}
                    name="settings.connectionString.connectionStringValue"
                    type="textarea"
                    as="textarea"
                    placeholder="Enter a connection string"
                    rows={5}
                />
            </div>
        );
    }

    if (authMethod === "entraId") {
        return (
            <div className="vstack gap-3">
                <div className="mb-2">
                    <div className="d-flex flex-grow align-items-baseline justify-content-between">
                        <FormLabel>Service Bus Namespace</FormLabel>
                        <PopoverWithHoverWrapper
                            message={
                                <>
                                    Example: <code>mynamespace.servicebus.windows.net</code>
                                </>
                            }
                        >
                            <small className="text-primary">
                                Format <Icon icon="help" margin="m-0" />
                            </small>
                        </PopoverWithHoverWrapper>
                    </div>
                    <FormInput
                        control={control}
                        name="settings.entraId.namespace"
                        type="text"
                        placeholder="Enter Service Bus Namespace (e.g. mynamespace.servicebus.windows.net)"
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>Tenant ID</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.entraId.tenantId"
                        type="text"
                        placeholder="Enter a Tenant ID"
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>Client ID</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.entraId.clientId"
                        type="text"
                        placeholder="Enter a Client ID"
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>Client Secret</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.entraId.clientSecret"
                        type="password"
                        placeholder="Enter a Client Secret"
                        passwordPreview
                    />
                </div>
            </div>
        );
    }

    if (authMethod === "passwordless") {
        return (
            <div className="mb-2">
                <div className="d-flex flex-grow align-items-baseline justify-content-between">
                    <FormLabel>Service Bus Namespace</FormLabel>
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Example: <code>mynamespace.servicebus.windows.net</code>
                            </>
                        }
                    >
                        <small className="text-primary">
                            Format <Icon icon="help" margin="m-0" />
                        </small>
                    </PopoverWithHoverWrapper>
                </div>
                <FormInput
                    control={control}
                    name="settings.passwordless.namespace"
                    type="text"
                    placeholder="Enter Service Bus Namespace (e.g. mynamespace.servicebus.windows.net)"
                />
            </div>
        );
    }

    assertUnreachable(authMethod);
}

const authenticationOptions: SelectOption<AzureServiceBusAuthenticationType>[] = [
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

function getStringRequiredSchema(authType: AzureServiceBusAuthenticationType) {
    return yup
        .string()
        .nullable()
        .when("$authType", {
            is: authType,
            then: (schema) => schema.required(),
        });
}

const schema = yupObjectSchema<FormData>({
    name: connectionStringsUtils.nameSchema,
    authType: yup.string<AzureServiceBusAuthenticationType>(),
    excludedDatabases: yup.array().of(yup.string()).optional(),
    settings: yupObjectSchema<FormData["settings"]>({
        connectionString: yupObjectSchema<FormData["settings"]["connectionString"]>({
            connectionStringValue: yup
                .string()
                .nullable()
                .when("$authType", {
                    is: "connectionString",
                    then: (schema) => schema.required(),
                }),
        }),
        entraId: yupObjectSchema<FormData["settings"]["entraId"]>({
            namespace: getStringRequiredSchema("entraId"),
            tenantId: getStringRequiredSchema("entraId"),
            clientId: getStringRequiredSchema("entraId"),
            clientSecret: getStringRequiredSchema("entraId"),
        }),
        passwordless: yupObjectSchema<FormData["settings"]["passwordless"]>({
            namespace: getStringRequiredSchema("passwordless"),
        }),
    }),
});

function getDefaultValues(initialConnection: AzureServiceBusConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            authType: "connectionString",
            settings: {
                connectionString: {
                    connectionStringValue: null,
                },
                entraId: {
                    namespace: null,
                    tenantId: null,
                    clientId: null,
                    clientSecret: null,
                },
                passwordless: {
                    namespace: null,
                },
            },
        };
    }

    return _.omit(initialConnection, "type", "usedBy");
}
