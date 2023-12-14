import { Button, Form, Label } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React from "react";
import { SubmitHandler, useFieldArray, useForm, useWatch } from "react-hook-form";
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

type FormData = ConnectionFormData<ElasticSearchConnection>;

export interface ElasticSearchStringProps extends EditConnectionStringFormProps {
    initialConnection: ElasticSearchConnection;
}

export default function ElasticSearchConnectionString({
    db,
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
    const { tasksService } = useServices();

    const asyncTest = useAsyncCallback(async (idx: number) => {
        const isValid = await trigger(`nodes.${idx}`);
        if (!isValid) {
            return;
        }

        const url = formValues.nodes[idx].url;
        return tasksService.testElasticSearchNodeConnection(db, url, mapElasticSearchAuthenticationToDto(formValues));
    });

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
                <div>
                    <Label className="mb-0 md-label">Nodes URLs</Label>
                    {formState.errors?.nodes?.message && (
                        <div className="text-danger small">{formState.errors.nodes.message}</div>
                    )}
                    {urlFieldArray.fields.map((urlField, idx) => (
                        <div key={urlField.id} className="d-flex mb-1 gap-1">
                            <FormInput
                                type="text"
                                control={control}
                                name={`nodes.${idx}.url`}
                                placeholder="http(s)://hostName"
                            />
                            <Button color="danger" onClick={() => urlFieldArray.remove(idx)}>
                                <Icon icon="trash" margin="m-0" title="Delete" />
                            </Button>
                            <ButtonWithSpinner
                                color="primary"
                                onClick={() => asyncTest.execute(idx)}
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
                <Button color="info" className="mt-1" onClick={() => urlFieldArray.append({ url: null })}>
                    <Icon icon="plus" />
                    Add URL
                </Button>
            </div>
            <div>
                <Label className="mb-0 md-label">Authentication</Label>
                <FormSelect
                    name="authMethodUsed"
                    control={control}
                    placeholder="Select an authentication option"
                    options={authenticationOptions}
                    isSearchable={false}
                />
            </div>
            {formValues.authMethodUsed === "Basic" && (
                <>
                    <div>
                        <Label className="mb-0 md-label">Username</Label>
                        <FormInput control={control} name="username" type="text" placeholder="Enter a username" />
                    </div>
                    <div>
                        <Label className="mb-0 md-label">Password</Label>
                        <FormInput control={control} name="password" type="password" placeholder="Enter a password" />
                    </div>
                </>
            )}
            {formValues.authMethodUsed === "API Key" && (
                <>
                    <div>
                        <Label className="mb-0 md-label">API Key ID</Label>
                        <FormInput control={control} name="apiKeyId" type="text" placeholder="Enter an API Key ID" />
                    </div>
                    <div>
                        <Label className="mb-0 md-label">API Key</Label>
                        <FormInput control={control} name="apiKey" type="text" placeholder="Enter an API Key" />
                    </div>
                </>
            )}
            {formValues.authMethodUsed === "Encoded API Key" && (
                <div>
                    <Label className="mb-0 md-label">Encoded API Key</Label>
                    <FormInput
                        control={control}
                        name="encodedApiKey"
                        type="text"
                        placeholder="Enter an encoded API Key"
                    />
                </div>
            )}
            {formValues.authMethodUsed === "Certificate" && (
                <div>
                    <Label className="mb-0 md-label w-100">Certificate file</Label>
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
                </div>
            )}
            <ConnectionStringUsedByTasks
                tasks={initialConnection.usedByTasks}
                urlProvider={forCurrentDatabase.editElasticSearchEtl}
            />
            <ConnectionTestResult testResult={asyncTest.result} />
        </Form>
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
