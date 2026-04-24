import { yupResolver } from "@hookform/resolvers/yup";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormGroup, FormInput, FormLabel, FormRadio, FormSelect, FormSwitch } from "components/common/Form";
import { FlexGrow } from "components/common/FlexGrow";
import { Icon } from "components/common/Icon";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import Modal from "components/common/Modal";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import { SelectOption } from "components/common/select/Select";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useFieldArray, useForm, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import Card from "react-bootstrap/Card";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import * as yup from "yup";

export default function CertificatesRegisterSsoUserModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();

    const isClusterAdminOrClusterNode = useAppSelector(accessManagerSelectors.isClusterAdminOrClusterNode);
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));
    const ssoServerCertificates = useAppSelector(certificatesSelectors.ssoServerCertificates);
    const ssoUserToEdit = useAppSelector(certificatesSelectors.ssoUserToEdit);

    const isEditing = ssoUserToEdit != null;

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            ssoUserId: ssoUserToEdit?.Thumbprint ?? "",
            ssoServerPublicKeyPinningHashes: ssoUserToEdit?.SsoServerPublicKeyPinningHashes ?? [],
            allowAnySso: ssoUserToEdit?.AllowAnySsoServer ?? false,
            securityClearance:
                (ssoUserToEdit?.SecurityClearance as "ValidUser" | "Operator" | "ClusterAdmin") ?? "ValidUser",
            databasePermissions: isEditing ? certificatesUtils.mapDatabasePermissionsFromDto(ssoUserToEdit) : [],
        },
    });

    const { control, formState, handleSubmit, reset } = form;
    const formValues = useWatch({ control });

    const permissionsFieldArray = useFieldArray({ control, name: "databasePermissions" });

    const handleClose = () => dispatch(certificatesActions.isRegisterSsoUserModalOpenToggled());

    const handleSubmitForm: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            const permissions =
                formData.securityClearance === "Operator" || formData.securityClearance === "ClusterAdmin"
                    ? null
                    : Object.fromEntries(
                          formData.databasePermissions.map(({ databaseName, accessLevel }) => [
                              databaseName,
                              accessLevel,
                          ])
                      );

            if (isEditing) {
                reportEvent("certificates", "edit-sso-user");

                await manageServerService.updateCertificate(
                    {
                        Name: ssoUserToEdit.Name,
                        Thumbprint: ssoUserToEdit.Thumbprint,
                        SecurityClearance: formData.securityClearance,
                        Permissions: permissions,
                        TwoFactorAuthenticationKey: null,
                        SsoServerPublicKeyPinningHashes: formData.allowAnySso
                            ? []
                            : formData.ssoServerPublicKeyPinningHashes,
                        AllowAnySsoServer: formData.allowAnySso,
                    },
                    false
                );
            } else {
                reportEvent("certificates", "register-sso-user");

                await manageServerService.registerSsoUserEntry({
                    Name: `SSO User: ${formData.ssoUserId}`,
                    Thumbprint: formData.ssoUserId,
                    Usage: "SsoClient",
                    SsoServerPublicKeyPinningHashes: formData.allowAnySso
                        ? []
                        : formData.ssoServerPublicKeyPinningHashes,
                    AllowAnySsoServer: formData.allowAnySso,
                    SecurityClearance: formData.securityClearance,
                    Permissions: permissions ?? {},
                });
            }

            reset(formData);
            dispatch(certificatesActions.fetchData());
            handleClose();
        });
    };

    const ssoServerOptions = ssoServerCertificates.map((cert) => ({
        value: cert.PublicKeyPinningHash,
        label: cert.Name,
    })) satisfies SelectOption[];

    return (
        <Modal show size="lg" centered contentClassName="modal-border bulge-primary">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleSubmitForm)}>
                    <Modal.Header className="vstack gap-4" onCloseClick={handleClose}>
                        <div className="text-center">
                            <Icon
                                icon="user"
                                addon={isEditing ? "edit" : "plus"}
                                className="fs-1"
                                color="primary"
                                margin="m-0"
                            />
                        </div>
                        <div className="text-center lead">
                            {isEditing ? "Edit SSO user" : "Generate SSO user"}
                        </div>
                    </Modal.Header>
                    <Modal.Body>
                        <FormGroup>
                            <div className="hstack gap-1">
                                <FormLabel>User ID</FormLabel>
                                <ConditionalPopover
                                    conditions={{
                                        isActive: true,
                                        message: (
                                            <>
                                                The way the user will identify.
                                                <br />
                                                Available formats:
                                                <ul className="mb-0">
                                                    <li>username,</li>
                                                    <li>email,</li>
                                                    <li>domain + username</li>
                                                </ul>
                                            </>
                                        ),
                                    }}
                                    popoverPlacement="right"
                                >
                                    <Icon icon="info" margin="m-0" className="text-muted small" />
                                </ConditionalPopover>
                            </div>
                            {isEditing ? (
                                <Form.Control type="text" value={ssoUserToEdit.Thumbprint} disabled />
                            ) : (
                                <FormInput
                                    control={control}
                                    type="text"
                                    name="ssoUserId"
                                    placeholder="e.g. john@example.com"
                                />
                            )}
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>Security clearance</FormLabel>
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
                        <hr />
                        <FormGroup>
                            <div className="hstack justify-content-between align-items-center mb-2">
                                <FormLabel className="mb-0">Authorizing SSO</FormLabel>
                                <FormSwitch control={control} name="allowAnySso">
                                    Allow any SSO to authorize
                                </FormSwitch>
                            </div>
                            <Collapse in={!formValues.allowAnySso}>
                                <div>
                                    <FormSelect
                                        control={control}
                                        name="ssoServerPublicKeyPinningHashes"
                                        isMulti
                                        placeholder="Select one or more SSO servers..."
                                        options={ssoServerOptions}
                                    />
                                </div>
                            </Collapse>
                        </FormGroup>
                        <hr />
                        <FormLabel>Database permissions</FormLabel>
                        {(formValues.securityClearance === "Operator" ||
                            formValues.securityClearance === "ClusterAdmin") && (
                            <FormGroup>
                                <RichAlert variant="info">
                                    With security clearance set to{" "}
                                    <strong>
                                        {formValues.securityClearance === "ClusterAdmin" ? "Cluster Admin" : "Operator"}
                                    </strong>
                                    , the user will have access to all databases.
                                </RichAlert>
                            </FormGroup>
                        )}
                        <Collapse in={formValues.securityClearance === "ValidUser"}>
                            <div>
                                <FormGroup>
                                    <div className="hstack gap-2">
                                        <SelectCreatable
                                            className="flex-grow-1"
                                            placeholder="Select (or enter a database)"
                                            isClearable
                                            options={allDatabaseNames
                                                .filter(
                                                    (x) =>
                                                        !(formValues.databasePermissions ?? [])
                                                            .map((p) => p.databaseName)
                                                            .includes(x)
                                                )
                                                .map((x) => ({ value: x, label: x }))}
                                            onChange={(value) =>
                                                permissionsFieldArray.append({
                                                    databaseName: value.value,
                                                    accessLevel: "ReadWrite",
                                                })
                                            }
                                            isClearedAfterSelect
                                            isDisabled={formState.isSubmitting}
                                        />
                                        <Button variant="primary" className="rounded-pill" disabled>
                                            <Icon icon="plus" margin="m-0" />
                                            Add
                                        </Button>
                                    </div>
                                </FormGroup>
                                <FormGroup className="vstack gap-2">
                                    {permissionsFieldArray.fields.map((field, idx) => (
                                        <Card key={field.id} className="hstack rounded px-3 py-1 well">
                                            {field.databaseName}
                                            <FlexGrow />
                                            <div className="hstack gap-3">
                                                <FormRadio
                                                    control={control}
                                                    name={`databasePermissions.${idx}.accessLevel`}
                                                    value="Admin"
                                                    className="text-success"
                                                    color="secondary"
                                                >
                                                    Admin
                                                </FormRadio>
                                                <FormRadio
                                                    control={control}
                                                    name={`databasePermissions.${idx}.accessLevel`}
                                                    value="ReadWrite"
                                                    className="text-warning"
                                                    color="secondary"
                                                >
                                                    Read/Write
                                                </FormRadio>
                                                <ConditionalPopover
                                                    conditions={{
                                                        isActive: !hasReadOnlyCertificates,
                                                        message: (
                                                            <LicenseRestrictedMessage>
                                                                Current license doesn&apos;t include
                                                                <br />
                                                                <strong className="text-info">
                                                                    <Icon icon="access-read" margin="m-0" /> Read-only
                                                                    certificates
                                                                </strong>
                                                            </LicenseRestrictedMessage>
                                                        ),
                                                    }}
                                                >
                                                    <FormRadio
                                                        control={control}
                                                        name={`databasePermissions.${idx}.accessLevel`}
                                                        value="Read"
                                                        className="text-info"
                                                        color="secondary"
                                                        disabled={!hasReadOnlyCertificates}
                                                    >
                                                        Read
                                                    </FormRadio>
                                                </ConditionalPopover>
                                            </div>
                                            <Button
                                                variant="link"
                                                className="px-0 ms-3"
                                                onClick={() => permissionsFieldArray.remove(idx)}
                                            >
                                                <Icon icon="trash" margin="m-0" className="text-danger" />
                                            </Button>
                                        </Card>
                                    ))}
                                </FormGroup>
                            </div>
                        </Collapse>
                    </Modal.Body>
                    <Modal.Footer>
                        <Button variant="link" onClick={handleClose} className="link-muted">
                            Cancel
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            variant="primary"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                            icon={isEditing ? undefined : "user"}
                        >
                            {isEditing ? "Save changes" : "Generate user"}
                        </ButtonWithSpinner>
                    </Modal.Footer>
                </Form>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    ssoUserId: yup.string().required(),
    ssoServerPublicKeyPinningHashes: yup
        .array()
        .of(yup.string())
        .when("allowAnySso", {
            is: false,
            then: (s) => s.min(1, "Select at least one SSO server"),
        }),
    allowAnySso: yup.boolean().required(),
    securityClearance: yup.string<"ValidUser" | "Operator" | "ClusterAdmin">().required(),
    databasePermissions: certificatesUtils.databasePermissionsSchema,
});

type FormData = yup.InferType<typeof schema>;
