import { Alert, Button, Form, Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { SubmitHandler, useFieldArray, useForm } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConnectionFormData, EditConnectionStringFormProps, RavenConnection } from "../connectionStringsTypes";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { useAsyncCallback } from "react-async-hook";
import { useAppUrls } from "components/hooks/useAppUrls";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ConnectionStringError from "./shared/ConnectionStringError";
import { yupObjectSchema } from "components/utils/yupUtils";

type FormData = ConnectionFormData<RavenConnection>;

export interface RavenConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: RavenConnection;
}

export default function RavenConnectionString({
    db,
    initialConnection,
    isForNewConnection,
    onSave,
}: RavenConnectionStringProps) {
    const { control, handleSubmit, formState, watch } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const urlFieldArray = useFieldArray({
        name: "topologyDiscoveryUrls",
        control,
    });

    const { forCurrentDatabase } = useAppUrls();
    const { databasesService } = useServices();

    const asyncTest = useAsyncCallback((idx: number) => {
        const url = watch("topologyDiscoveryUrls")[idx].url;
        return databasesService.testClusterNodeConnection(url, db.name, false);
    });

    const isTestDisabled = (idx: number) => {
        return (
            asyncTest.loading ||
            !watch("topologyDiscoveryUrls")[idx] ||
            !!formState.errors?.topologyDiscoveryUrls?.[idx]
        );
    };

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "Raven",
            ...formData,
        } satisfies RavenConnection);
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
                <Label className="mb-0 md-label">Database</Label>
                <FormInput
                    control={control}
                    name="database"
                    type="text"
                    placeholder="Enter database for the connection string"
                />
            </div>
            <div>
                <div>
                    <Label className="mb-0 md-label">Discovery URLs</Label>
                    {formState.errors?.topologyDiscoveryUrls?.message && (
                        <div className="text-danger small">{formState.errors.topologyDiscoveryUrls.message}</div>
                    )}
                    {urlFieldArray.fields.map((urlField, idx) => (
                        <div key={urlField.id} className="d-flex mb-1 gap-1">
                            <FormInput
                                type="text"
                                control={control}
                                name={`topologyDiscoveryUrls.${idx}.url`}
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
            </div>
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editRavenEtl}
            />
            {asyncTest.result?.Success && <Alert color="success">Successfully connected</Alert>}
            {asyncTest.result?.Error && (
                <>
                    <ConnectionStringError message={asyncTest.result.Error} />
                    <AboutError isHTTPSuccess={asyncTest.result.HTTPSuccess} />
                </>
            )}
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

const schema = yupObjectSchema<FormData>({
    name: yup.string().nullable().required(),
    database: yup.string().nullable().required(),
    topologyDiscoveryUrls: yup
        .array()
        .of(yup.object({ url: yup.string().basicUrl().nullable().required() }))
        .min(1),
});

const yupSchemaResolver = yupResolver(schema);

function getDefaultValues(initialConnection: RavenConnection, isForNewConnection: boolean): FormData {
    if (isForNewConnection) {
        return {
            name: null,
            database: null,
            topologyDiscoveryUrls: [{ url: null }],
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
