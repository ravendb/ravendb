import { Form, Label } from "reactstrap";
import { FormInput } from "components/common/Form";
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
import { yupObjectSchema } from "components/utils/yupUtils";

type FormData = ConnectionFormData<RabbitMqConnection>;

interface RabbitMqConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: RabbitMqConnection;
}

export default function RabbitMqConnectionString({
    db,
    initialConnection,
    isForNewConnection,
    onSave,
}: RabbitMqConnectionStringProps) {
    const { control, handleSubmit, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();
    const { databasesService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger("connectionString");
        if (!isValid) {
            return;
        }

        return databasesService.testRabbitMqServerConnection(db, formValues.connectionString);
    });

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "RabbitMQ",
            ...formData,
        } satisfies RabbitMqConnection);
    };

    return (
        <Form id="connection-string-form" onSubmit={handleSubmit(handleSave)} className="vstack gap-2">
            <div>
                <Label className="mb-0 md-label">Name</Label>
                <FormInput
                    control={control}
                    name="name"
                    type="text"
                    placeholder="Enter a name for the connection string"
                    disabled={!isForNewConnection}
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Connection string</Label>
                <FormInput
                    control={control}
                    name="connectionString"
                    type="textarea"
                    rows={3}
                    placeholder="Enter a connection string for RabbitMQ"
                />
            </div>
            <div>
                <ButtonWithSpinner
                    color="primary"
                    icon="rocket"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                >
                    Test Connection
                </ButtonWithSpinner>
            </div>
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editRabbitMqEtl}
            />
            <ConnectionTestResult testResult={asyncTest.result} />
        </Form>
    );
}

const schema = yupObjectSchema<FormData>({
    name: yup.string().nullable().required(),
    connectionString: yup.string().nullable().required(),
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
