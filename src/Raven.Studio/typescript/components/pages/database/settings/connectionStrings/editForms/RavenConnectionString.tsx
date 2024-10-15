import { Badge, Button, Form, Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { Control, SubmitHandler, UseFormTrigger, UseFormWatch, useFieldArray, useForm } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConnectionFormData, EditConnectionStringFormProps, RavenConnection } from "../connectionStringsTypes";
import { useServices } from "components/hooks/useServices";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { yupResolver } from "@hookform/resolvers/yup";
import * as yup from "yup";
import { useAsyncCallback } from "react-async-hook";
import { useAppUrls } from "components/hooks/useAppUrls";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ConnectionTestError from "../../../../../common/connectionTests/ConnectionTestError";
import { yupObjectSchema } from "components/utils/yupUtils";
import RichAlert from "components/common/RichAlert";

type FormData = ConnectionFormData<RavenConnection>;

export interface RavenConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: RavenConnection;
}

export default function RavenConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: RavenConnectionStringProps) {
    const { control, handleSubmit, formState, watch, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const urlFieldArray = useFieldArray({
        name: "topologyDiscoveryUrls",
        control,
    });

    const { forCurrentDatabase } = useAppUrls();

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "Raven",
            ...formData,
        } satisfies RavenConnection);
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
                <Label>Database</Label>
                <FormInput
                    control={control}
                    name="database"
                    type="text"
                    placeholder="Enter database for the connection string"
                    autoComplete="off"
                />
            </div>
            <div className="mb-2">
                <Label>Discovery URLs</Label>
                <div className="vstack gap-3">
                    {formState.errors?.topologyDiscoveryUrls?.message && (
                        <div className="text-danger small">{formState.errors.topologyDiscoveryUrls.message}</div>
                    )}
                    {urlFieldArray.fields.map((urlField, idx) => (
                        <DiscoveryUrl
                            key={urlField.id}
                            idx={idx}
                            control={control}
                            isDeleteButtonVisible={urlFieldArray.fields.length > 1}
                            onDelete={() => urlFieldArray.remove(idx)}
                            trigger={trigger}
                            watch={watch}
                        />
                    ))}
                </div>
                <Button color="info" className="mt-3" onClick={() => urlFieldArray.append({ url: null })}>
                    <Icon icon="plus" />
                    Add next discovery URL
                </Button>
            </div>
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editRavenEtl}
            />
        </Form>
    );
}

interface DiscoveryUrlsProps {
    idx: number;
    control: Control<FormData>;
    isDeleteButtonVisible: boolean;
    onDelete: () => void;
    trigger: UseFormTrigger<FormData>;
    watch: UseFormWatch<FormData>;
}

function DiscoveryUrl({ idx, control, isDeleteButtonVisible, trigger, watch, onDelete }: DiscoveryUrlsProps) {
    const { tasksService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger([`topologyDiscoveryUrls.${idx}`, "database"]);
        if (!isValid) {
            return;
        }

        const url = watch(`topologyDiscoveryUrls.${idx}.url`);
        const databaseName = watch("database");
        return tasksService.testClusterNodeConnection(url, databaseName, false);
    });

    return (
        <div className="vstack mb-2 gap-1">
            <Label className="mb-0 d-flex align-items-center gap-1">
                <span className="small-label mb-0">URL #{idx + 1}</span>
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
            <div className="input-group mb-2">
                <FormInput
                    type="text"
                    control={control}
                    name={`topologyDiscoveryUrls.${idx}.url`}
                    placeholder="http(s)://hostName"
                    autoComplete="off"
                />
                {isDeleteButtonVisible && (
                    <Button color="danger" title="Delete URL" onClick={onDelete} disabled={asyncTest.loading}>
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                )}
                <ButtonWithSpinner
                    color="secondary"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                    title="Test connection"
                    icon="rocket"
                >
                    Test connection
                </ButtonWithSpinner>
            </div>
            {asyncTest.result?.Error && (
                <div className="vstack gap-1 mt-3">
                    <ConnectionTestError message={asyncTest.result.Error} />
                    <AboutError isHTTPSuccess={asyncTest.result.HTTPSuccess} />
                </div>
            )}
        </div>
    );
}

function AboutError({ isHTTPSuccess }: { isHTTPSuccess: boolean }) {
    return (
        <RichAlert variant="info" title="About this error">
            <p>
                Each RavenDB server has both HTTP and TCP endpoints. While the first one is used for system management
                and client-server rest request, the second is used for inter-server and advanced client-server
                communications.
            </p>
            <p>The connection tests the TCP endpoint only after a successful HTTP connection.</p>

            {isHTTPSuccess ? (
                <p>
                    It appears that the current server was able to connect to the desired server through HTTP, but
                    failed connecting to it using TCP.
                </p>
            ) : (
                <p>It appears that the current server could not connect to the desired node through HTTP.</p>
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
        </RichAlert>
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
