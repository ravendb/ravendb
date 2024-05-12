import { Badge, Button, Form, Label } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React from "react";
import { SubmitHandler, UseFormTrigger, useFieldArray, useForm, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";
import {
    ConnectionFormData,
    EditConnectionStringFormProps,
    ElasticSearchAuthenticationMethod,
    ElasticSearchConnection,
} from "../connectionStringsTypes";
import { yupObjectSchema } from "components/utils/yupUtils";
import * as yup from "yup";
import { yupResolver } from "@hookform/resolvers/yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import fileImporter from "common/fileImporter";
import certificateUtils from "common/certificateUtils";
import messagePublisher from "common/messagePublisher";
import forge = require("node-forge");
import { mapElasticSearchAuthenticationToDto } from "../store/connectionStringsMapsToDto";
import ConnectionTestResult from "../../../../../common/connectionTests/ConnectionTestResult";
import ConnectionStringUsedByTasks from "./shared/ConnectionStringUsedByTasks";
import { useAppUrls } from "components/hooks/useAppUrls";
import ElasticSearchCertificate from "./ElasticSearchCertificate";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";

type FormData = ConnectionFormData<ElasticSearchConnection>;

export interface ElasticSearchStringProps extends EditConnectionStringFormProps {
    initialConnection: ElasticSearchConnection;
}

export default function ElasticSearchConnectionString({
    initialConnection,
    isForNewConnection,
    onSave,
}: ElasticSearchStringProps) {
    const { control, formState, handleSubmit, setValue, trigger } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const urlFieldArray = useFieldArray({
        name: "nodes",
        control,
    });

    const formValues = useWatch({ control });
    const { forCurrentDatabase } = useAppUrls();

    const onCertificateUploaded = (data: string) => {
        const currentCertificates = formValues.certificatesBase64 ?? [];

        try {
            // First detect the data format, pfx (binary) or crt/cer (text)
            // The line bellow will throw if data is not pfx
            forge.asn1.fromDer(data);

            // *** Handle pfx ***
            try {
                const certAsBase64 = forge.util.encode64(data);
                const extractBase64s = certificateUtils
                    .extractCertificatesFromPkcs12(certAsBase64, undefined)
                    .map((x) => certificateUtils.extractBase64(x));

                setValue("certificatesBase64", [...currentCertificates, ...extractBase64s]);
            } catch ($ex1) {
                messagePublisher.reportError("Unable to upload certificate", $ex1);
            }
        } catch {
            // *** Handle crt/cer ***
            try {
                setValue("certificatesBase64", [...currentCertificates, certificateUtils.extractBase64(data)]);
            } catch ($ex2) {
                messagePublisher.reportError("Unable to upload certificate", $ex2);
            }
        }
    };

    const handleSave: SubmitHandler<FormData> = (formData: FormData) => {
        onSave({
            type: "ElasticSearch",
            ...formData,
        } satisfies ElasticSearchConnection);
    };

    const deleteCertificate = (cert: string) => {
        setValue(
            "certificatesBase64",
            formValues.certificatesBase64.filter((x) => x !== cert)
        );
    };

    const isUploadCertificateVisible = !formValues.certificatesBase64 || formValues.certificatesBase64.length === 0;

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
                <Label>Nodes URLs</Label>
                <div className="vstack gap-3">
                    {formState.errors?.nodes?.message && (
                        <div className="text-danger small">{formState.errors.nodes.message}</div>
                    )}
                    {urlFieldArray.fields.map((urlField, idx) => (
                        <NodeUrl
                            key={urlField.id}
                            idx={idx}
                            control={control}
                            formValues={formValues}
                            isDeleteButtonVisible={urlFieldArray.fields.length > 1}
                            onDelete={() => urlFieldArray.remove(idx)}
                            trigger={trigger}
                        />
                    ))}
                </div>
                <Button color="info" className="mt-4" onClick={() => urlFieldArray.append({ url: null })}>
                    <Icon icon="plus" />
                    Add URL
                </Button>
            </div>
            <div className="mb-2">
                <Label>Authentication</Label>
                <FormSelect
                    name="authMethodUsed"
                    control={control}
                    placeholder="Select an authentication option"
                    options={authenticationOptions}
                    isSearchable={false}
                />
            </div>
            {formValues.authMethodUsed === "Basic" && (
                <div className="vstack gap-3">
                    <div className="mb-2">
                        <Label>Username</Label>
                        <FormInput
                            control={control}
                            name="username"
                            type="text"
                            placeholder="Enter a username"
                            autoComplete="off"
                        />
                    </div>
                    <div className="mb-2">
                        <Label>Password</Label>
                        <FormInput
                            control={control}
                            name="password"
                            type="password"
                            placeholder="Enter a password"
                            autoComplete="off"
                        />
                    </div>
                </div>
            )}
            {formValues.authMethodUsed === "API Key" && (
                <div className="vstack gap-3">
                    <div className="mb-2">
                        <Label>API Key ID</Label>
                        <FormInput
                            control={control}
                            name="apiKeyId"
                            type="text"
                            placeholder="Enter an API Key ID"
                            autoComplete="off"
                        />
                    </div>
                    <div className="mb-2">
                        <Label>API Key</Label>
                        <FormInput
                            control={control}
                            name="apiKey"
                            type="text"
                            placeholder="Enter an API Key"
                            autoComplete="off"
                        />
                    </div>
                </div>
            )}
            {formValues.authMethodUsed === "Encoded API Key" && (
                <div className="mb-2">
                    <Label>Encoded API Key</Label>
                    <FormInput
                        control={control}
                        name="encodedApiKey"
                        type="text"
                        placeholder="Enter an encoded API Key"
                        autoComplete="off"
                    />
                </div>
            )}
            {formValues.authMethodUsed === "Certificate" && (
                <div className="mb-2">
                    <Label>Certificate file</Label>
                    {isUploadCertificateVisible && (
                        <div>
                            <Label className="btn btn-primary">
                                <Icon icon="upload" />
                                Upload existing certificate
                                <input
                                    type="file"
                                    className="d-none"
                                    id="elasticCertificateFilePicker"
                                    onChange={(e) =>
                                        fileImporter.readAsBinaryString(e.currentTarget, (x) =>
                                            onCertificateUploaded(x)
                                        )
                                    }
                                />
                            </Label>
                        </div>
                    )}
                    {formValues.certificatesBase64?.map((cert) => (
                        <ElasticSearchCertificate
                            key={cert}
                            certBase64={cert}
                            onDelete={() => deleteCertificate(cert)}
                        />
                    ))}
                    {formState.errors?.certificatesBase64 && (
                        <div className="text-danger small">{formState.errors.certificatesBase64.message}</div>
                    )}
                </div>
            )}
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editElasticSearchEtl}
            />
        </Form>
    );
}

interface NodeUrlProps {
    idx: number;
    control: any;
    formValues: FormData;
    isDeleteButtonVisible: boolean;
    onDelete: () => void;
    trigger: UseFormTrigger<FormData>;
}

function NodeUrl({ idx, control, formValues, isDeleteButtonVisible, onDelete, trigger }: NodeUrlProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { tasksService } = useServices();

    const asyncTest = useAsyncCallback(async () => {
        const isValid = await trigger(`nodes.${idx}`);
        if (!isValid) {
            return;
        }

        const url = formValues.nodes[idx].url;
        return tasksService.testElasticSearchNodeConnection(
            databaseName,
            url,
            mapElasticSearchAuthenticationToDto(formValues)
        );
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
            <div className="input-group">
                <FormInput
                    type="text"
                    control={control}
                    name={`nodes.${idx}.url`}
                    placeholder="http(s)://hostName"
                    autoComplete="off"
                />
                {isDeleteButtonVisible && (
                    <Button color="danger" onClick={onDelete} disabled={asyncTest.loading}>
                        <Icon icon="trash" margin="m-0" title="Delete URL" />
                    </Button>
                )}
                <ButtonWithSpinner
                    color="secondary"
                    onClick={asyncTest.execute}
                    isSpinning={asyncTest.loading}
                    icon={{
                        icon: "rocket",
                        title: "Test connection",
                    }}
                >
                    Test connection
                </ButtonWithSpinner>
            </div>
            {asyncTest.result?.Error && (
                <div className="mt-3">
                    <ConnectionTestResult testResult={asyncTest.result} />
                </div>
            )}
        </div>
    );
}

const authenticationOptions: SelectOption[] = exhaustiveStringTuple<ElasticSearchAuthenticationMethod>()(
    "No authentication",
    "Basic",
    "API Key",
    "Encoded API Key",
    "Certificate"
).map((type: any) => ({
    value: type,
    label: type,
}));

const schema = yupObjectSchema<FormData>({
    name: yup.string().nullable().required(),
    authMethodUsed: yup.string<ElasticSearchAuthenticationMethod>(),
    apiKey: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "API Key",
            then: (schema) => schema.required(),
        }),
    apiKeyId: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "API Key",
            then: (schema) => schema.required(),
        }),
    encodedApiKey: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "Encoded API Key",
            then: (schema) => schema.required(),
        }),
    password: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "Basic",
            then: (schema) => schema.required(),
        }),
    username: yup
        .string()
        .nullable()
        .when("authMethodUsed", {
            is: "Basic",
            then: (schema) => schema.required(),
        }),
    certificatesBase64: yup
        .array()
        .nullable()
        .of(yup.string())
        .when("authMethodUsed", {
            is: "Certificate",
            then: (schema) => schema.min(1),
        }),
    nodes: yup
        .array()
        .of(yup.object({ url: yup.string().basicUrl().nullable().required() }))
        .min(1),
});

const yupSchemaResolver = yupResolver(schema);

function getDefaultValues(initialConnection: any, isForNewConnection: any): FormData {
    if (isForNewConnection) {
        return {
            authMethodUsed: "No authentication",
            apiKey: null,
            apiKeyId: null,
            encodedApiKey: null,
            password: null,
            username: null,
            certificatesBase64: [],
            nodes: [{ url: null }],
        };
    }

    return _.omit(initialConnection, "type", "usedByTasks");
}
