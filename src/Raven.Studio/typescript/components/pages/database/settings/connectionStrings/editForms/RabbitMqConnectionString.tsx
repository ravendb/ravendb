import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";

import { FormInput, FormLabel } from "components/common/Form";
import React from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { ConnectionFormData, EditConnectionStringFormProps, RabbitMqConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionTestResult from "../../../../../common/connectionTests/ConnectionTestResult";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ExcludedDatabasesFormSelect from "./shared/ExcludedDatabasesFormSelect";
import { yupObjectSchema } from "components/utils/yupUtils";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { connectionStringSelectors } from "../store/connectionStringsSlice";
import { ConnectionStringsNameContext, connectionStringsUtils } from "../connectionStringsUtils";

type FormData = ConnectionFormData<RabbitMqConnection>;

interface RabbitMqConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: RabbitMqConnection;
}

export default function RabbitMqConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: RabbitMqConnectionStringProps) {
    const usedNames = useAppSelector(connectionStringSelectors.connections)["RabbitMQ"].map((x) => x.name);
    const isServerWide = useAppSelector(connectionStringSelectors.isServerWide);

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
    const { forCurrentDatabase } = useAppUrls();
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("connectionString");
        if (!isValid) {
            return;
        }

        return isServerWide
            ? tasksService.testServerWideRabbitMqServerConnection(formValues.connectionString)
            : tasksService.testRabbitMqServerConnection(databaseName, formValues.connectionString);
    });

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "RabbitMQ",
            ...formData,
        } satisfies RabbitMqConnection);
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
                    as="textarea"
                    rows={3}
                    placeholder="Enter a connection string for RabbitMQ"
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
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editRabbitMqEtl}
            />
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

function getDefaultValues(initialConnection: RabbitMqConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            connectionString: null,
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
