import { yupResolver } from "@hookform/resolvers/yup";
import copyToClipboard from "common/copyToClipboard";
import genUtils from "common/generalUtils";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormCheckbox, FormInput, FormRadio, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { LazyLoad } from "components/common/LazyLoad";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { QRCode } from "qrcodejs";
import { ElementRef, useEffect, useRef, useState } from "react";
import { useAsync } from "react-async-hook";
import { SubmitHandler, useFieldArray, useForm, useWatch } from "react-hook-form";
import {
    Button,
    Card,
    CardBody,
    CardHeader,
    Collapse,
    Form,
    FormGroup,
    Label,
    Modal,
    ModalBody,
    ModalFooter,
} from "reactstrap";
import * as yup from "yup";
type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
type DatabaseAccess = Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess;
import endpoints from "endpoints";
import notificationCenter from "common/notifications/notificationCenter";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { SelectOption } from "components/common/select/Select";

export default function CertificatesGenerateModal() {
    const dispatch = useAppDispatch();
    const { manageServerService, databasesService } = useServices();
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));

    const downloadCertFormRef = useRef<HTMLFormElement>(null);

    const { control, handleSubmit, reset, setValue } = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            name: "",
            securityClearance: "ValidUser",
            certificatePassphrase: "",
            expireIn: null,
            expireTimeUnits: "months",
            databasePermissions: [],
            isRequire2FA: false,
            authenticationKey: "",
        },
    });

    const formValues = useWatch({ control });

    const permissionsFieldArray = useFieldArray({
        control,
        name: "databasePermissions",
    });

    const qrContainerRef = useRef<ElementRef<"div">>(null);
    const [qrCode, setQrCode] = useState<typeof QRCode>(null);

    const { loading: isAuthenticationKeyLoading } = useAsync(async () => {
        if (formValues.isRequire2FA) {
            const result = await manageServerService.generateTwoFactorSecret();
            setValue("authenticationKey", result.Secret);
        }
    }, [formValues.isRequire2FA]);

    useEffect(() => {
        const generateQrCode = async () => {
            if (!formValues.authenticationKey) {
                qrCode?.clear();
                return;
            }

            const encodedIssuer = encodeURIComponent(location.hostname);
            const encodedName = encodeURIComponent(formValues.name ?? "Client Certificate");

            const uri = `otpauth://totp/${encodedIssuer}:${encodedName}?secret=${formValues.authenticationKey}&issuer=${encodedIssuer}`;

            if (!qrCode) {
                setQrCode(
                    new QRCode(qrContainerRef.current, {
                        text: uri,
                        width: 256,
                        height: 256,
                        colorDark: "#000000",
                        colorLight: "#ffffff",
                        correctLevel: QRCode.CorrectLevel.Q,
                    })
                );
            } else {
                qrCode.clear();
                qrCode.makeCode(uri);
            }
        };

        generateQrCode();

        return () => {
            qrCode?.clear();
        };
    }, [formValues.authenticationKey, formValues.name, qrCode]);

    const handleGenerate: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            const operationId = await databasesService.getNextOperationId(null);

            const url = `${endpoints.global.adminCertificates.adminCertificates}?operationId=${operationId}&raft-request-id=${genUtils.generateUUID()}`;
            downloadCertFormRef.current.setAttribute("action", url);
            downloadCertFormRef.current.submit();

            try {
                await notificationCenter.instance.monitorOperation(null, operationId);
                reset(formData);
                dispatch(certificatesActions.fetchData(null));
                dispatch(certificatesActions.isGenerateModalOpenToggled());
            } catch {
                notificationCenter.instance.openDetailsForOperationById(null, operationId);
            }
        });
    };

    return (
        <Modal isOpen wrapClassName="bs5" size="lg" centered contentClassName="modal-border bulge-success">
            <Form onSubmit={handleSubmit(handleGenerate)}>
                <ModalBody>
                    <div className="text-center">
                        <Icon icon="magic-wand" className="fs-1" margin="m-0" />
                    </div>
                    <div className="position-absolute m-2 end-0 top-0">
                        <Button close onClick={() => dispatch(certificatesActions.isGenerateModalOpenToggled())} />
                    </div>
                    <div className="text-center lead">Generate client certificate</div>
                    <FormGroup>
                        <Label>Name</Label>
                        <FormInput type="text" control={control} name="name" />
                    </FormGroup>
                    <FormGroup>
                        <Label>Security Clearance</Label>
                        <FormSelect
                            control={control}
                            name="securityClearance"
                            options={
                                [
                                    {
                                        value: "ClusterAdmin",
                                        label: "Cluster Admin",
                                        isDisabled: !isClusterAdminOrClusterNode,
                                    },
                                    { value: "Operator", label: "Operator" },
                                    { value: "ValidUser", label: "User" },
                                ] satisfies SelectOption[]
                            }
                        />
                    </FormGroup>
                    <FormGroup>
                        <Label>Certificate Passphrase</Label>
                        <FormInput type="password" control={control} name="certificatePassphrase" passwordPreview />
                    </FormGroup>
                    <FormGroup>
                        <Label>Expire in</Label>
                        <FormInput
                            type="number"
                            control={control}
                            name="expireIn"
                            placeholder="Validity period (Default: 60 months)"
                            addon={
                                <FormSelect
                                    control={control}
                                    name="expireTimeUnits"
                                    options={
                                        [
                                            { value: "days", label: "Days" },
                                            { value: "months", label: "Months" },
                                        ] satisfies SelectOption[]
                                    }
                                />
                            }
                        />
                    </FormGroup>
                    <hr />
                    Database Permissions
                    <AccessToAllDatabasesInfo securityClearance={formValues.securityClearance} />
                    <Collapse isOpen={formValues.securityClearance === "ValidUser"}>
                        <FormGroup>
                            <SelectCreatable
                                placeholder="Select (or enter) a database"
                                isClearable
                                options={allDatabaseNames
                                    .filter(
                                        (x) => !formValues.databasePermissions.map((x) => x.databaseName).includes(x)
                                    )
                                    .map((x) => ({
                                        value: x,
                                        label: x,
                                    }))}
                                onChange={(value) =>
                                    permissionsFieldArray.append({
                                        databaseName: value.value,
                                        accessLevel: "ReadWrite",
                                    })
                                }
                                isClearedAfterSelect
                            />
                        </FormGroup>
                        <FormGroup className="vstack gap-2">
                            {permissionsFieldArray.fields.map((field, idx) => (
                                <Card key={field.id} className="rounded">
                                    <CardHeader className="d-flex justify-content-between p-1">
                                        <div>{field.databaseName}</div>
                                        <Button color="link" onClick={() => permissionsFieldArray.remove(idx)}>
                                            <Icon icon="trash" margin="m-0" className="text-danger" />
                                        </Button>
                                    </CardHeader>
                                    <CardBody className="d-flex gap-2 p-1 well rounded">
                                        <FormRadio
                                            control={control}
                                            name={`databasePermissions.${idx}.accessLevel`}
                                            value="Admin"
                                            className="text-success"
                                        >
                                            Admin
                                        </FormRadio>
                                        <FormRadio
                                            control={control}
                                            name={`databasePermissions.${idx}.accessLevel`}
                                            value="ReadWrite"
                                            className="text-warning"
                                        >
                                            Read/Write
                                        </FormRadio>
                                        <ConditionalPopover
                                            conditions={{
                                                isActive: !hasReadOnlyCertificates,
                                                message: <LicenseRestrictedBadge licenseRequired="Professional +" />,
                                            }}
                                        >
                                            <FormRadio
                                                control={control}
                                                name={`databasePermissions.${idx}.accessLevel`}
                                                value="Read"
                                                className="text-danger"
                                                disabled={!hasReadOnlyCertificates}
                                            >
                                                Read
                                            </FormRadio>
                                        </ConditionalPopover>
                                    </CardBody>
                                </Card>
                            ))}
                        </FormGroup>
                    </Collapse>
                    <FormGroup>
                        <FormCheckbox control={control} name="isRequire2FA">
                            Require two-factor authentication
                        </FormCheckbox>
                    </FormGroup>
                    <Collapse isOpen={formValues.isRequire2FA}>
                        <LazyLoad active={isAuthenticationKeyLoading}>
                            <FormGroup>
                                <Label>Authentication Key</Label>
                                <FormInput
                                    type="text"
                                    control={control}
                                    name="authenticationKey"
                                    addon={
                                        <Button
                                            onClick={() =>
                                                copyToClipboard.copy(
                                                    formValues.authenticationKey,
                                                    "Authentication Key was copied to clipboard."
                                                )
                                            }
                                            color="link"
                                        >
                                            <Icon icon="copy" margin="m-0" />
                                        </Button>
                                    }
                                    disabled
                                />
                            </FormGroup>
                            <FormGroup>
                                <Label>QR Code:</Label>
                                <br />
                                <div ref={qrContainerRef} className="qrcode" />
                            </FormGroup>
                        </LazyLoad>
                    </Collapse>
                </ModalBody>
                <ModalFooter>
                    <Button
                        color="link"
                        onClick={() => dispatch(certificatesActions.isGenerateModalOpenToggled())}
                        className="link-muted"
                    >
                        Cancel
                    </Button>
                    <Button type="submit" color="success" onClick={() => {}} className="rounded-pill">
                        Generate
                    </Button>
                </ModalFooter>
            </Form>

            {/* This form is used to download certificate */}
            <form ref={downloadCertFormRef} className="d-none" />
        </Modal>
    );
}

function AccessToAllDatabasesInfo({ securityClearance }: { securityClearance: SecurityClearance }) {
    if (securityClearance === "Operator" || securityClearance === "ClusterAdmin") {
        const getClearanceLabel = () => {
            if (securityClearance === "ClusterAdmin") {
                return "Cluster Admin";
            }

            if (securityClearance === "Operator") {
                return "Operator";
            }

            return null;
        };

        return (
            <FormGroup>
                <RichAlert variant="info">
                    With security clearance set to <strong>{getClearanceLabel()}</strong>, the user of this certificate
                    will have access to all databases.
                </RichAlert>
            </FormGroup>
        );
    }

    return null;
}

const schema = yup.object({
    name: yup.string().required(),
    securityClearance: yup.string<SecurityClearance>().required(),
    certificatePassphrase: yup.string(),
    expireIn: yup.number().nullable().positive().integer(),
    expireTimeUnits: yup.string<"days" | "months">(),
    databasePermissions: yup
        .array()
        .of(yup.object({ databaseName: yup.string(), accessLevel: yup.string<DatabaseAccess>() })),
    isRequire2FA: yup.boolean(),
    authenticationKey: yup.string().when("isRequire2FA", {
        is: true,
        then: (schema) => schema.required(),
    }),
});

type FormData = yup.InferType<typeof schema>;
