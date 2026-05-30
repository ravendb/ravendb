import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import { FormInput, FormLabel } from "components/common/Form";
import React from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { ConnectionFormData, EditConnectionStringFormProps, SnowflakeConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ExcludedDatabasesFormSelect from "./shared/ExcludedDatabasesFormSelect";
import { useAsyncCallback } from "react-async-hook";
import ConnectionTestResult from "../../../../../common/connectionTests/ConnectionTestResult";
import { Icon } from "components/common/Icon";
import { yupObjectSchema } from "components/utils/yupUtils";
import { FlexGrow } from "components/common/FlexGrow";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";

type FormData = ConnectionFormData<SnowflakeConnection>;

export interface SnowflakeConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: SnowflakeConnection;
}

export default function SnowflakeConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: SnowflakeConnectionStringProps) {
    const usedNames = useAppSelector(connectionStringSelectors.connections)["Snowflake"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
        context: {
            isForNewConnection,
            usedNames,
        } satisfies ConnectionStringsNameContext,
    });

    const formValues = useWatch({ control });
    const { tasksService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(["connectionString"]);
        if (!isValid) {
            return;
        }

        return isServerWide
            ? tasksService.testServerWideSnowflakeConnectionString(formValues.connectionString)
            : tasksService.testSnowflakeConnectionString(databaseName, formValues.connectionString);
    });

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "Snowflake",
            ...formData,
        } satisfies SnowflakeConnection);
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
                    Connection string{" "}
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
                <FormInput
                    control={control}
                    name="connectionString"
                    type="textarea"
                    placeholder="Enter connection string"
                    rows={3}
                    autoComplete="off"
                />
                <div className="d-flex mt-4">
                    <FlexGrow />
                    <ButtonWithSpinner
                        variant="secondary"
                        icon="rocket"
                        onClick={asyncTest.execute}
                        isSpinning={asyncTest.loading}
                    >
                        Test connection
                    </ButtonWithSpinner>
                </div>
            </div>
            <ConnectionStringUsedByTasks tasks={initialConnection.usedBy} connectionType={initialConnection.type} />
            {isServerWide && <ExcludedDatabasesFormSelect control={control} name="excludedDatabases" />}
            {asyncTest.result?.Error && <ConnectionTestResult testResult={asyncTest.result} />}
        </Form>
    );
}

const schema = yupObjectSchema<FormData>({
    name: connectionStringsUtils.nameSchema,
    connectionString: yup.string().nullable().required(),
    excludedDatabases: yup.array().of(yup.string()).optional(),
});

const yupSchemaResolver = yupResolver(schema);

function getDefaultValues(initialConnection: SnowflakeConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            connectionString: null,
        };
    }

    return _.omit(initialConnection, "type", "usedBy");
}
