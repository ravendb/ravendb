import { Alert, Button, Form, Label, ModalBody, ModalFooter } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { EditConnectionStringFormProps, RavenDbConnection } from "../connectionStringsTypes";
import { tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { useDispatch } from "react-redux";
import { connectionStringsActions } from "../store/connectionStringsSlice";
import { useAsyncCallback } from "react-async-hook";
import { useAppUrls } from "components/hooks/useAppUrls";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ConnectionStringError from "./shared/ConnectionStringError";
import EditConnectionStringFormFooter from "./shared/ConnectionStringFormFooter";

export interface RavenConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: RavenDbConnection;
}

export default function RavenConnectionString({
    db,
    initialConnection,
    isForNewConnection,
}: RavenConnectionStringProps) {
    const dispatch = useDispatch();

    const { control, handleSubmit, formState, watch } = useForm<FormData>({
        mode: "all",
        defaultValues: {
            Name: initialConnection.Name,
            Database: initialConnection.Database,
            TopologyDiscoveryUrls: initialConnection.TopologyDiscoveryUrls?.map((x) => ({ url: x })) ?? [{ url: "" }],
        },
        resolver: yupSchemaResolver,
    });

    const urlFieldArray = useFieldArray({
        name: "TopologyDiscoveryUrls",
        control,
    });

    const { forCurrentDatabase } = useAppUrls();
    const { databasesService } = useServices();

    const asyncTest = useAsyncCallback((idx: number) => {
        const url = watch("TopologyDiscoveryUrls")[idx].url;
        return databasesService.testClusterNodeConnection(url, db.name, false);
    });

    const isTestDisabled = (idx: number) => {
        return (
            asyncTest.loading ||
            !watch("TopologyDiscoveryUrls")[idx] ||
            !!formState.errors?.TopologyDiscoveryUrls?.[idx]
        );
    };

    const onSave: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            const TopologyDiscoveryUrls = formData.TopologyDiscoveryUrls.map((x) => x.url);

            const newConnection: RavenDbConnection = {
                ...formData,
                Type: "Raven",
                TopologyDiscoveryUrls,
            };

            await databasesService.saveConnectionString(db, newConnection);

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
                    <Label className="mb-0 md-label">Database</Label>
                    <FormInput
                        control={control}
                        name="Database"
                        type="text"
                        placeholder="Enter database for the connection string"
                    />
                </div>
                <div>
                    <div>
                        <Label className="mb-0 md-label">Discovery URLs</Label>
                        {formState.errors?.TopologyDiscoveryUrls?.message && (
                            <div className="text-danger small">{formState.errors.TopologyDiscoveryUrls.message}</div>
                        )}
                        {urlFieldArray.fields.map((urlField, idx) => (
                            <div key={urlField.id} className="d-flex mb-1 gap-1">
                                <FormInput
                                    type="text"
                                    control={control}
                                    name={`TopologyDiscoveryUrls.${idx}.url`}
                                    placeholder="http(s)://hostName"
                                />
                                <Button color="danger" onClick={() => urlFieldArray.remove(idx)}>
                                    <Icon icon="trash" margin="m-0" title="Delete" />
                                </Button>
                                <ButtonWithSpinner
                                    color="primary"
                                    onClick={() => asyncTest.execute(idx)}
                                    disabled={isTestDisabled(idx)}
                                    isSpinning={asyncTest.loading && asyncTest.currentParams?.[0] === idx}
                                    icon={{
                                        icon: "rocket",
                                        title: "Test connection",
                                        margin: "m-0",
                                    }}
                                />
                            </div>
                        ))}
                    </div>
                    <Button color="info" className="mt-1" onClick={() => urlFieldArray.append({ url: "" })}>
                        <Icon icon="plus" />
                        Add URL
                    </Button>
                    <ConnectionStringUsedByTasks
                        tasks={initialConnection.UsedByTasks}
                        urlProvider={forCurrentDatabase.editRavenEtl}
                    />
                </div>
                {asyncTest.result?.Success && <Alert color="success">Successfully connected</Alert>}
                {asyncTest.result?.Error && (
                    <>
                        <ConnectionStringError message={asyncTest.result.Error} />
                        <AboutError isHTTPSuccess={asyncTest.result.HTTPSuccess} />
                    </>
                )}
            </ModalBody>
            <EditConnectionStringFormFooter isSubmitting={formState.isSubmitting} />
        </Form>
    );
}

function AboutError({ isHTTPSuccess }: { isHTTPSuccess: boolean }) {
    return (
        <Alert color="info">
            <h4>About this error</h4>
            <div>
                Each RavenDB server has both HTTP and TCP endpoints. While the first one is used for system management
                and client-server rest request, the second is used for inter-server and advanced client-server
                communications.
            </div>
            <div>The connection tests the TCP endpoint only after a successful HTTP connection.</div>

            {isHTTPSuccess ? (
                <div>
                    It appears that the current server was able to connect to the desired server through HTTP, but
                    failed connecting to it using TCP.
                </div>
            ) : (
                <div>It appears that the current server could not connect to the desired node through HTTP.</div>
            )}

            <div>
                Please verify that:
                <ul>
                    <li>The URL is correct</li>
                    <li>Both RavenDB and the target machine are up and running</li>
                    <li>There are no firewall settings on either machine blocking usage of that URL</li>
                    <li>There are no network configurations that prevent communication</li>
                </ul>
            </div>
        </Alert>
    );
}

const schema = yup
    .object({
        Name: yup.string().nullable().required(),
        Database: yup.string().nullable().required(),
        TopologyDiscoveryUrls: yup
            .array()
            .of(yup.object({ url: yup.string().basicUrl().nullable().required() }))
            .min(1),
    })
    .required();

const yupSchemaResolver = yupResolver(schema);
type FormData = Required<yup.InferType<typeof schema>>;
