import { Form, Label, ModalBody, UncontrolledTooltip } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import { EditConnectionStringFormProps, RabbitMqConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { useDispatch } from "react-redux";
import EditConnectionStringFormFooter from "./shared/ConnectionStringFormFooter";
import { tryHandleSubmit } from "components/utils/common";
import { connectionStringsActions } from "../store/connectionStringsSlice";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import ConnectionStringTestResult from "./shared/ConnectionStringTestResult";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";

interface RabbitMqConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: RabbitMqConnection;
}

export default function RabbitMqConnectionString({
    db,
    initialConnection,
    isForNewConnection,
}: RabbitMqConnectionStringProps) {
    const dispatch = useDispatch();

    const { control, handleSubmit, formState } = useForm<FormData>({
        defaultValues: {
            Name: initialConnection.Name,
            ConnectionString: initialConnection.RabbitMqConnectionSettings?.ConnectionString,
        },
        resolver: yupSchemaResolver,
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();
    const { databasesService } = useServices();

    const asyncTest = useAsyncCallback(() => {
        return databasesService.testRabbitMqServerConnection(db, formValues.ConnectionString);
    });

    const isTestButtonDisabled = !formValues.ConnectionString;

    const onSave: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            await databasesService.saveConnectionString(db, {
                Type: "Queue",
                BrokerType: "RabbitMq",
                Name: formData.Name,
                RabbitMqConnectionSettings: {
                    ConnectionString: formData.ConnectionString,
                },
                KafkaConnectionSettings: null,
            });

            const newConnection: RabbitMqConnection = {
                ...formData,
                Type: "RabbitMQ",
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
                        disabled={!isForNewConnection}
                    />
                </div>
                <div>
                    <Label className="mb-0 md-label">Connection string</Label>
                    <FormInput
                        control={control}
                        name="ConnectionString"
                        type="textarea"
                        rows={3}
                        placeholder="Enter a connection string for RabbitMQ"
                    />
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
                        <UncontrolledTooltip target={testButtonId}>Enter a connection string.</UncontrolledTooltip>
                    )}
                </div>
                <ConnectionStringUsedByTasks
                    tasks={initialConnection.UsedByTasks}
                    urlProvider={forCurrentDatabase.editRavenEtl}
                />
                <ConnectionStringTestResult testResult={asyncTest.result} />
            </ModalBody>
            <EditConnectionStringFormFooter isSubmitting={formState.isSubmitting} />
        </Form>
    );
}

const testButtonId = "test-button";

const schema = yup
    .object({
        Name: yup.string().nullable().required(),
        ConnectionString: yup.string().nullable().required(),
    })
    .required();

const yupSchemaResolver = yupResolver(schema);
type FormData = Required<yup.InferType<typeof schema>>;
