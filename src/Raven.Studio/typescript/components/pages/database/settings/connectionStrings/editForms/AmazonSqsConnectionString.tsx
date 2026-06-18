import React, { useEffect } from "react";
import { AmazonSqsConnection, ConnectionFormData, EditConnectionStringFormProps } from "../connectionStringsTypes";
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
import ExcludedDatabasesFormSelect from "components/pages/database/settings/connectionStrings/editForms/shared/ExcludedDatabasesFormSelect";
import { useServices } from "components/hooks/useServices";
import ConnectionTestResult from "components/common/connectionTests/ConnectionTestResult";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { Icon } from "components/common/Icon";
import { mapAmazonSqsConnectionStringSettingsToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";

type FormData = ConnectionFormData<AmazonSqsConnection>;

export interface AmazonSqsConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AmazonSqsConnection;
}

export default function AmazonSqsConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: AmazonSqsConnectionStringProps) {
    const usedNames = useAppSelector(connectionStringSelectors.connections)["AmazonSqs"].map((x) => x.name);
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

        const dto = mapAmazonSqsConnectionStringSettingsToDto(formValues);
        return isServerWide
            ? tasksService.testServerWideAmazonSqsServerConnection(dto)
            : tasksService.testAmazonSqsServerConnection(databaseName, dto);
    });

    // Clear test result after changing auth type
    useEffect(() => {
        asyncTest.set(null);
    }, [formValues.authType]);

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "AmazonSqs",
            ...formData,
        } satisfies AmazonSqsConnection);
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
            {isServerWide && (
                <ExcludedDatabasesFormSelect
                    control={control}
                    name="excludedDatabases"
                    usedBy={initialConnection.usedBy}
                />
            )}
        </Form>
    );
}

interface SelectedAuthFieldsProps {
    control: Control<FormData>;
    authMethod: AmazonSqsAuthenticationType;
}

function SelectedAuthFields({ control, authMethod }: SelectedAuthFieldsProps) {
    if (authMethod === "basic") {
        return (
            <div className="vstack gap-3">
                <div className="mb-2">
                    <FormLabel>Access Key</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.basic.accessKey"
                        type="text"
                        placeholder="Enter an Access Key"
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>Secret Key</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.basic.secretKey"
                        type="password"
                        placeholder="Enter a Secret Key"
                        passwordPreview
                    />
                </div>
                <div className="mb-2">
                    <FormLabel>Region Name</FormLabel>
                    <FormInput
                        control={control}
                        name="settings.basic.regionName"
                        type="text"
                        placeholder="Enter a Region Name"
                    />
                </div>
            </div>
        );
    }

    return null;
}

const authenticationOptions: SelectOption<AmazonSqsAuthenticationType>[] = [
    {
        value: "basic",
        label: "Basic",
    },
    {
        value: "passwordless",
        label: "Passwordless",
    },
];

function getStringRequiredSchema(authType: AmazonSqsAuthenticationType) {
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
    authType: yup.string<AmazonSqsAuthenticationType>(),
    excludedDatabases: yup.array().of(yup.string()).optional(),
    settings: yupObjectSchema<FormData["settings"]>({
        basic: yupObjectSchema<FormData["settings"]["basic"]>({
            accessKey: getStringRequiredSchema("basic"),
            secretKey: getStringRequiredSchema("basic"),
            regionName: getStringRequiredSchema("basic"),
        }),
        passwordless: yup.boolean(),
    }),
});

function getDefaultValues(initialConnection: AmazonSqsConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            authType: "basic",
            settings: {
                basic: {
                    accessKey: null,
                    secretKey: null,
                    regionName: null,
                },
                passwordless: false,
            },
        };
    }

    return _.omit(initialConnection, "type", "usedBy");
}
