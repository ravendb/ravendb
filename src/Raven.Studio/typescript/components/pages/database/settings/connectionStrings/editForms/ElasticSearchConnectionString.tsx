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
import replicationCertificateModel from "models/database/tasks/replicationCertificateModel";
import forge = require("node-forge");
import { mapElasticSearchAuthenticationToDto } from "../store/connectionStringsMapsToDto";

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
    const { control, formState, handleSubmit, setValue } = useForm<FormData>({
        mode: "all",
        defaultValues: getDefaultValues(initialConnection, isForNewConnection),
        resolver: yupSchemaResolver,
    });

    const urlFieldArray = useFieldArray({
        name: "nodes",
        control,
    });

    const formValues = useWatch({ control });

    console.log("kalczur formValues", formValues);

    const { databasesService } = useServices();

    const asyncTest = useAsyncCallback((idx: number) => {
        const url = formValues.nodes[idx].url;
        return databasesService.testElasticSearchNodeConnection(
            db,
            url,
            mapElasticSearchAuthenticationToDto(formValues)
        );
    });

    const isTestDisabled = (idx: number) => {
        return asyncTest.loading || !formValues.nodes[idx] || !!formState.errors?.nodes?.[idx];
    };

    const onCertificateUploaded = (data: string) => {
        try {
            // First detect the data format, pfx (binary) or crt/cer (text)
            // The line bellow will throw if data is not pfx
            forge.asn1.fromDer(data);

            // *** Handle pfx ***
            try {
                const certAsBase64 = forge.util.encode64(data);
                const certificatesArray = certificateUtils.extractCertificatesFromPkcs12(certAsBase64, undefined);

                certificatesArray.forEach((publicKey) => {
                    const certificateModel = new replicationCertificateModel(publicKey, certAsBase64);

                    // TODO map to cert object like in 5.4
                    setValue("certificatesBase64", [...formValues.certificatesBase64, certificateModel.publicKey()]);
                });
            } catch ($ex1) {
                messagePublisher.reportError("Unable to upload certificate", $ex1);
            }
        } catch {
            // *** Handle crt/cer ***
            try {
                const certificateModel = new replicationCertificateModel(data);
                setValue("certificatesBase64", [...formValues.certificatesBase64, certificateModel.publicKey()]);
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
                    {formValues.certificatesBase64.length === 0 && (
                        <div>
                            <Label className="btn btn-primary">
                                <Icon icon="upload" />
                                Upload existing certificate
                                <input
                                    type="file"
                                    className="d-none"
                                    id="elasticCertificateFilePicker"
                                    onChange={(e) =>
                                        fileImporter.readAsBinaryString(e.target, (x) => onCertificateUploaded(x))
                                    }
                                />
                            </Label>
                        </div>
                    )}
                    {formValues.certificatesBase64.map((cert) => (
                        <div key={cert}>{cert}</div>
                    ))}
                </div>
            )}
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
        .of(yup.string())
        .when("authMethodUsed", {
            is: "Certificate",
            then: (schema) => schema.required(),
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
