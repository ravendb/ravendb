import { Button, Form, Label, ModalBody, UncontrolledTooltip } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { SubmitHandler, useFieldArray, useForm, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConnectionFormData, EditConnectionStringFormProps, KafkaConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ConnectionTestResult from "../../../../../common/connectionTests/ConnectionTestResult";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

type FormData = ConnectionFormData<KafkaConnection>;

interface KafkaConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: KafkaConnection;
}

export default function KafkaConnectionString({
    db,
    initialConnection,
    isForNewConnection,
    onSave,
}: KafkaConnectionStringProps) {
    const { control, handleSubmit, formState } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const connectionOptionsFieldArray = useFieldArray({
        name: "connectionOptions",
        control,
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();
    const { databasesService } = useServices();

    const asyncTest = useAsyncCallback(() => {
        return databasesService.testKafkaServerConnection(
            db,
            formValues.bootstrapServers,
            false,
            getConnectionOptionsDto(formValues.connectionOptions)
        );
    });

    const isTestButtonDisabled = !formValues.bootstrapServers;

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            ...formData,
            type: "Kafka",
        } satisfies KafkaConnection);
    };

    return (
        <Form onSubmit={handleSubmit(handleSave)}>
            <div>
                <Label className="mb-0 md-label">Name</Label>
                <FormInput
                    control={control}
                    name="name"
                    type="text"
                    placeholder="Enter a name for the connection string"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Bootstrap Servers</Label>
                <FormInput
                    control={control}
                    name="bootstrapServers"
                    type="text"
                    placeholder="Enter comma-separated Bootstrap Servers"
                />
            </div>
            <div>
                <div>
                    <Label className="mb-0 md-label">
                        Connection Options <small className="text-muted fw-light">(optional)</small>
                    </Label>
                    {connectionOptionsFieldArray.fields.map((option, idx) => (
                        <div key={option.id} className="d-flex mb-1 gap-1">
                            <FormInput
                                type="text"
                                control={control}
                                name={`connectionOptions.${idx}.key`}
                                placeholder="Enter an option key"
                            />
                            <FormInput
                                type="text"
                                control={control}
                                name={`connectionOptions.${idx}.value`}
                                placeholder="Enter an option value"
                            />
                            <Button color="danger" onClick={() => connectionOptionsFieldArray.remove(idx)}>
                                <Icon icon="trash" margin="m-0" title="Delete" />
                            </Button>
                        </div>
                    ))}
                </div>
                <Button
                    color="info"
                    className="mt-1"
                    onClick={() => connectionOptionsFieldArray.append({ key: "", value: "" })}
                >
                    <Icon icon="plus" />
                    Add new connection option
                </Button>
                <div id={testButtonId} className="mt-2" style={{ width: "fit-content" }}>
                    <ButtonWithSpinner
                        color="primary"
                        icon="rocket"
                        onClick={asyncTest.execute}
                        disabled={isTestButtonDisabled}
                        isSpinning={asyncTest.loading}
                    >
                        Test Connection
                    </ButtonWithSpinner>
                </div>
                {isTestButtonDisabled && (
                    <UncontrolledTooltip target={testButtonId}>Enter bootstrap servers.</UncontrolledTooltip>
                )}
            </div>
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editKafkaEtl}
            />
            <ConnectionTestResult testResult={asyncTest.result} />
        </Form>
    );
}

const testButtonId = "test-button";

function getConnectionOptionsDto(connectionOptions: { key?: string; value?: string }[]): { [key: string]: string } {
    return Object.fromEntries(connectionOptions.map((x) => [x.key, x.value]));
}

const schema = yup
    .object({
        Name: yup.string().nullable().required(),
        BootstrapServers: yup.string().bootstrapConnections().nullable().required(),
        ConnectionOptions: yup
            .array()
            .of(yup.object({ key: yup.string().nullable().required(), value: yup.string().nullable().required() })),
    })
    .required();

const yupSchemaResolver = yupResolver(schema);

function getDefaultValues(initialConnection: KafkaConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            bootstrapServers: null,
            connectionOptions: [],
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
