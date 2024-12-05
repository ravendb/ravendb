import React, { useEffect } from "react";
import { ConnectionFormData, EditConnectionStringFormProps, AmazonSqsConnection } from "../connectionStringsTypes";
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
import { Icon } from "components/common/Icon";
import { mapAmazonSqsConnectionStringSettingsToDto } from "components/pages/database/settings/connectionStrings/store/connectionStringsMapsToDto";

type FormData = ConnectionFormData<AmazonSqsConnection>;

export interface AmazonSqsConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: AmazonSqsConnection;
}

export default function AmazonSqsConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: AmazonSqsConnectionStringProps) {
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

        return tasksService.testAmazonSqsServerConnection(
            databaseName,
            mapAmazonSqsConnectionStringSettingsToDto(formValues)
        );
    });

    // Clear test result after changing auth type
    useEffect(() => {
        asyncTest.set(null);
        // eslint-disable-next-line react-hooks/exhaustive-deps
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
                urlProvider={forCurrentDatabase.editAmazonSqsEtl}
            />
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
                    <Label>Access Key</Label>
                    <FormInput
                        control={control}
                        name="settings.basic.accessKey"
                        type="text"
                        placeholder="Enter an Access Key"
                    />
                </div>
                <div className="mb-2">
                    <Label>Secret Key</Label>
                    <FormInput
                        control={control}
                        name="settings.basic.secretKey"
                        type="password"
                        placeholder="Enter a Secret Key"
                        passwordPreview
                    />
                </div>
                <div className="mb-2">
                    <Label>Region Name</Label>
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
    name: yup.string().nullable().required(),
    authType: yup.string<AmazonSqsAuthenticationType>(),
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

    return _.omit(initialConnection, "type", "usedByTasks");
}
