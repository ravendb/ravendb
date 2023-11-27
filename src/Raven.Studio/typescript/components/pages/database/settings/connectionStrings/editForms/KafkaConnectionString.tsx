import { Button, Form, Label, ModalBody, UncontrolledTooltip } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { SubmitHandler, useFieldArray, useForm, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { tryHandleSubmit } from "components/utils/common";
import { EditConnectionStringFormProps, KafkaConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { useDispatch } from "react-redux";
import { connectionStringsActions } from "../store/connectionStringsSlice";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ConnectionStringTestResult from "./shared/ConnectionStringTestResult";
import ConnectionStringFormFooter from "./shared/ConnectionStringFormFooter";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

interface KafkaConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: KafkaConnection;
}

export default function KafkaConnectionString({
    db,
    initialConnection,
    isForNewConnection,
}: KafkaConnectionStringProps) {
    const dispatch = useDispatch();

    const { control, handleSubmit, formState } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection),
        resolver: yupSchemaResolver,
    });

    const connectionOptionsFieldArray = useFieldArray({
        name: "ConnectionOptions",
        control,
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();
    const { databasesService } = useServices();

    const asyncTest = useAsyncCallback(() => {
        return databasesService.testKafkaServerConnection(
            db,
            formValues.BootstrapServers,
            false,
            getConnectionOptionsDto(formValues.ConnectionOptions)
        );
    });

    const isTestButtonDisabled = !formValues.BootstrapServers;

    const onSave: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await databasesService.saveConnectionString(db, {
                Type: "Queue",
                BrokerType: "Kafka",
                Name: formData.Name,
                KafkaConnectionSettings: {
                    BootstrapServers: formData.BootstrapServers,
                    ConnectionOptions: getConnectionOptionsDto(formValues.ConnectionOptions),
                    UseRavenCertificate: initialConnection.KafkaConnectionSettings?.UseRavenCertificate ?? false,
                },
                RabbitMqConnectionSettings: null,
            });

            const newConnection: KafkaConnection = {
                ...formData,
                Type: "Kafka",
            };

            if (isForNewConnection) {
                dispatch(connectionStringsActions.addConnection(newConnection));
            } else {
                dispatch(
                    connectionStringsActions.editConnection({
                        oldName: initialConnection.Name,
                        newConnection,
                    })
                );
            }

            dispatch(connectionStringsActions.closeEditConnectionModal());
        });
    };
    return (
        <Form onSubmit={handleSubmit(onSave)}>
            <ModalBody className="vstack gap-3">
                <div>
                    <Label className="mb-0 md-label">Name</Label>
                    <FormInput
                        control={control}
                        name="Name"
                        type="text"
                        placeholder="Enter a name for the connection string"
                    />
                </div>
                <div>
                    <Label className="mb-0 md-label">Bootstrap Servers</Label>
                    <FormInput
                        control={control}
                        name="BootstrapServers"
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
                                    name={`ConnectionOptions.${idx}.key`}
                                    placeholder="Enter an option key"
                                />
                                <FormInput
                                    type="text"
                                    control={control}
                                    name={`ConnectionOptions.${idx}.value`}
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
                    tasks={initialConnection.UsedByTasks}
                    urlProvider={forCurrentDatabase.editRavenEtl}
                />

                <ConnectionStringTestResult testResult={asyncTest.result} />
            </ModalBody>

            <ConnectionStringFormFooter isSubmitting={formState.isSubmitting} />
        </Form>
    );
}

const testButtonId = "test-button";

function getDefaultValues(initialConnection: KafkaConnection): FormData {
    return {
        Name: initialConnection.Name,
        BootstrapServers: initialConnection.KafkaConnectionSettings?.BootstrapServers,
        ConnectionOptions: Object.keys(initialConnection.KafkaConnectionSettings?.ConnectionOptions ?? {}).map(
            (key) => ({
                key,
                value: initialConnection.KafkaConnectionSettings?.ConnectionOptions?.[key],
            })
        ),
    };
}

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
type FormData = Required<yup.InferType<typeof schema>>;
