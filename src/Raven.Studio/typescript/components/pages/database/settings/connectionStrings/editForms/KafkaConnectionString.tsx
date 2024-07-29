import { Badge, Button, Form, Label, PopoverBody, UncontrolledPopover } from "reactstrap";
import { FormInput, FormSwitch } from "components/common/Form";
import React, { useEffect } from "react";
import { SubmitHandler, useFieldArray, useForm, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { ConnectionFormData, EditConnectionStringFormProps, KafkaConnection } from "../connectionStringsTypes";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { yupObjectSchema } from "components/utils/yupUtils";
import ConnectionTestError from "components/common/connectionTests/ConnectionTestError";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

type FormData = ConnectionFormData<KafkaConnection>;

interface KafkaConnectionStringProps extends EditConnectionStringFormProps {
    initialConnection: KafkaConnection;
}

export default function KafkaConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: KafkaConnectionStringProps) {
    const { control, handleSubmit, trigger, setValue } = useForm<FormData>({
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
    const { tasksService } = useServices();

    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    useEffect(() => {
        if (
            formValues.isUseRavenCertificate &&
            !formValues.connectionOptions.map((x) => x.key).includes(sslCaLocation)
        ) {
            setValue("connectionOptions", [{ key: sslCaLocation, value: null }, ...formValues.connectionOptions]);
        }
    }, [formValues.connectionOptions, formValues.isUseRavenCertificate, setValue]);

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(["bootstrapServers", "connectionOptions"]);
        if (!isValid) {
            return;
        }

        return tasksService.testKafkaServerConnection(
            databaseName,
            formValues.bootstrapServers,
            false,
            getConnectionOptionsDto(formValues.connectionOptions)
        );
    });

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            ...formData,
            type: "Kafka",
        } satisfies KafkaConnection);
    };

    const isDeleteUrlVisible = (optionKey: string) => {
        return !(formValues.isUseRavenCertificate && optionKey === sslCaLocation);
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
                    Bootstrap Servers
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
                <div className="input-group">
                    <FormInput
                        control={control}
                        name="bootstrapServers"
                        type="text"
                        placeholder="Enter comma-separated Bootstrap Servers"
                        autoComplete="off"
                    />
                    <ButtonWithSpinner
                        color="secondary"
                        icon="rocket"
                        title="Test connection"
                        onClick={asyncTest.execute}
                        isSpinning={asyncTest.loading}
                        disabled={asyncTest.loading}
                    >
                        Test connection
                    </ButtonWithSpinner>
                </div>
            </div>
            {asyncTest.result?.Error && (
                <div className="vstack gap-1 mb-2">
                    <ConnectionTestError message={asyncTest.result.Error} />
                </div>
            )}
            {isSecureServer && (
                <div className="mb-2">
                    <FormSwitch control={control} name="isUseRavenCertificate">
                        <span className="d-flex align-items-center gap-1">
                            Use RavenDB Certificate <Icon icon="info" color="info" id="useCertInfo" />
                        </span>
                    </FormSwitch>
                    <UseCertificateInfoPopover />
                </div>
            )}
            <div className="mb-2">
                <Label>
                    Connection Options <small className="text-muted fw-light">(optional)</small>
                </Label>
                <div className="vstack gap-3">
                    {connectionOptionsFieldArray.fields.map((option, idx) => (
                        <div>
                            <div className="vstack mb-2 gap-1">
                                <Label className="mb-0 d-flex align-items-center gap-1">
                                    <span className="small-label mb-0">Connection Option #{idx + 1}</span>
                                </Label>
                                <div key={option.id} className="d-flex gap-1 mb-2">
                                    <FormInput
                                        type="text"
                                        control={control}
                                        name={`connectionOptions.${idx}.key`}
                                        placeholder="Enter an option key"
                                        autoComplete="off"
                                    />
                                    <FormInput
                                        type={isMultiLineKey(option.key) ? "textarea" : "text"}
                                        control={control}
                                        name={`connectionOptions.${idx}.value`}
                                        placeholder="Enter an option value"
                                        autoComplete="off"
                                    />
                                    {isDeleteUrlVisible(option.key) && (
                                        <Button color="danger" onClick={() => connectionOptionsFieldArray.remove(idx)}>
                                            <Icon icon="trash" margin="m-0" title="Delete" />
                                        </Button>
                                    )}
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
                <Button
                    color="info"
                    className={connectionOptionsFieldArray.fields.length > 0 ? "mt-3" : "mt-1"}
                    onClick={() => connectionOptionsFieldArray.append({ key: null, value: null })}
                >
                    <Icon icon="plus" />
                    Add new connection option
                </Button>
            </div>
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editKafkaEtl}
            />
        </Form>
    );
}

function getConnectionOptionsDto(connectionOptions: { key?: string; value?: string }[]): Record<string, string> {
    return Object.fromEntries(connectionOptions.map((x) => [x.key, x.value]));
}

const connectionOptionSchema = yup.object({
    key: yup.string().nullable().required(),
    value: yup.string().nullable().required(),
});

const schema = yupObjectSchema<FormData>({
    name: yup.string().nullable().required(),
    bootstrapServers: yup
        .string()
        .nullable()
        .required()
        .test("bootstrap-connections", "Format should be: 'hostA:portNumber,hostB:portNumber,...'", (value) => {
            if (!value) {
                return true;
            }

            const values = value.split(",");
            return values.every((x) => x.match(/^[a-zA-Z0-9\-_.]+:\d+$/));
        })
        .test("no-protocol", "A bootstrap server cannot start with http/https", (value) => {
            if (!value) {
                return true;
            }

            const values = value.split(",");
            return values.every((x) => !x.startsWith("http"));
        }),
    connectionOptions: yup.array().of(connectionOptionSchema),
    isUseRavenCertificate: yup.boolean(),
});

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

const sslCaLocation = "ssl.ca.location";
const multiLineKeys: string[] = [
    "ssl.keystore.key",
    "ssl.keystore.certificate.chain",
    "ssl.truststore.certificates",
    "ssl.key.pem",
    "ssl.certificate.pem",
    "ssl.ca.pem",
];

function isMultiLineKey(key: string) {
    return multiLineKeys.includes(key);
}

function UseCertificateInfoPopover() {
    return (
        <UncontrolledPopover placement="right" trigger="hover" target="useCertInfo">
            <PopoverBody>
                <div>
                    The following <strong>configuration options</strong> will be set for you when using RavenDB server
                    certificate:
                </div>
                <ul>
                    <li>
                        <code>security.protocol = SSL</code>
                    </li>
                    <li>
                        <code>ssl.key.pem = &lt;RavenDB Server Private Key&gt;</code>
                    </li>
                    <li>
                        <code>ssl.certificate.pem = &lt;RavenDB Server Public Key&gt;</code>
                    </li>
                </ul>
            </PopoverBody>
        </UncontrolledPopover>
    );
}
