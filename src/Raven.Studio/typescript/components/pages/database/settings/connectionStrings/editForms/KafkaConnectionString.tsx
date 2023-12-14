import { Button, Form, Label, UncontrolledTooltip } from "reactstrap";
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
import ConnectionTestResult from "../../../../../common/connectionTests/ConnectionTestResult";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { yupObjectSchema } from "components/utils/yupUtils";
import { useAccessManager } from "components/hooks/useAccessManager";

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
    const isSecureServer = useAccessManager().isSecuredServer();

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
            db,
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

    return (
        <Form id="connection-string-form" onSubmit={handleSubmit(handleSave)} className="vstack gap-2">
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
            {isSecureServer && (
                <div>
                    <FormSwitch control={control} name="isUseRavenCertificate">
                        Use RavenDB Certificate <Icon icon="info" color="info" id="use-cert-info" />
                    </FormSwitch>
                    <UseCertificateInfoTooltip />
                </div>
            )}
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
                                type={isMultiLineKey(option.key) ? "textarea" : "text"}
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
                    onClick={() => connectionOptionsFieldArray.append({ key: null, value: null })}
                >
                    <Icon icon="plus" />
                    Add new connection option
                </Button>
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
                urlProvider={forCurrentDatabase.editKafkaEtl}
            />
            <ConnectionTestResult testResult={asyncTest.result} />
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

function UseCertificateInfoTooltip() {
    return (
        <UncontrolledTooltip placement="right" target="use-cert-info">
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
        </UncontrolledTooltip>
    );
}
