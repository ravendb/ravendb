import { yupResolver } from "@hookform/resolvers/yup";
import copyToClipboard from "common/copyToClipboard";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormCheckbox, FormInput, FormRadio, FormSelect, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { LazyLoad } from "components/common/LazyLoad";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
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

// TODO move common fileds to separate files

export default function CertificatesRegenerateModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));

    const certificate = useAppSelector(certificatesSelectors.regenerateModalData).certificate;

    const { control, handleSubmit, reset, setValue } = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            name: certificate.Name,
            securityClearance: certificate.SecurityClearance,
            certificatePassphrase: "",
            expireIn: null,
            expireTimeUnits: "months",
            databasePermissions: Object.entries(certificate.Permissions).map(([databaseName, access]) => ({
                databaseName,
                accessLevel: access,
            })),
            isDeleteExisting: false,
        },
    });

    const formValues = useWatch({ control });

    const permissionsFieldArray = useFieldArray({
        control,
        name: "databasePermissions",
    });

    const handleRegenerate: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            // todo;
            console.log("kalczur formData", formData);

            reset(formData);
        });
    };

    return (
        <Modal isOpen wrapClassName="bs5" size="lg" centered contentClassName="modal-border bulge-success">
            <Form onSubmit={handleSubmit(handleRegenerate)}>
                <ModalBody>
                    <div className="text-center">
                        <Icon icon="refresh" className="fs-1" margin="m-0" />
                    </div>
                    <div className="position-absolute m-2 end-0 top-0">
                        <Button close onClick={() => dispatch(certificatesActions.regenerateModalClosed())} />
                    </div>
                    <div className="text-center lead">Regenerate client certificate</div>
                    <FormGroup>
                        <Label>Name</Label>
                        <FormInput type="text" control={control} name="name" />
                    </FormGroup>
                    <FormGroup>
                        <Label>Security Clearance</Label>
                        <FormSelect
                            control={control}
                            name="securityClearance"
                            options={[
                                { value: "ClusterAdmin", label: "Cluster Admin" }, // TODO disable when !isClusterAdminOrClusterNode
                                { value: "Operator", label: "Operator" },
                                { value: "ValidUser", label: "User" },
                            ]}
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
                                    options={[
                                        { value: "days", label: "Days" },
                                        { value: "months", label: "Months" },
                                    ]}
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
                    <hr />
                    <FormGroup>
                        <FormSwitch
                            control={control}
                            name="isDeleteExisting"
                            title="Existing certificate will be deleted"
                        >
                            Delete existing certificate
                            <br />(<code>{certificate.Thumbprint}</code>)
                        </FormSwitch>
                    </FormGroup>
                </ModalBody>
                <ModalFooter>
                    <Button
                        color="link"
                        onClick={() => dispatch(certificatesActions.regenerateModalClosed())}
                        className="link-muted"
                    >
                        Cancel
                    </Button>
                    <Button type="submit" color="success" onClick={() => {}} className="rounded-pill">
                        Generate
                    </Button>
                </ModalFooter>
            </Form>
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
    isDeleteExisting: yup.boolean(),
});

type FormData = yup.InferType<typeof schema>;
