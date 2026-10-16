import { yupResolver } from "@hookform/resolvers/yup";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import Button from "react-bootstrap/Button";
import * as yup from "yup";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import CertificatesPermissionsField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesPermissionsField";
import Certificates2FAField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/Certificates2FAField";
import CertificatesSecurityClearanceField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesSecurityClearanceField";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import useCertificatePermissionsConfirm from "components/pages/resources/manageServer/certificates/utils/useCertificatePermissionsConfirm";
import { FormGroup, FormLabel } from "components/common/Form";
import Modal from "components/common/Modal";
import Form from "react-bootstrap/Form";
import useUniqueId from "components/hooks/useUniqueId";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesEditModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();
    const permissionsConfirm = useCertificatePermissionsConfirm();
    const titleId = useUniqueId("edit-certificate-title");

    const certificate = useAppSelector(certificatesSelectors.certificateToEdit);

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            securityClearance: certificate.SecurityClearance,
            databasePermissions: certificatesUtils.mapDatabasePermissionsFromDto(certificate),
            isRequire2FA: certificate.HasTwoFactor,
            isRegenerate2FAKey: false,
            authenticationKey: "",
        },
        context: {
            certHasTwoFactor: certificate.HasTwoFactor,
        },
    });

    const { formState, handleSubmit, reset } = form;

    const close = () => dispatch(certificatesActions.editModalClosed());

    const handleEdit: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "edit");

            const isPermissionConfirmed = await permissionsConfirm(formData);
            if (!isPermissionConfirmed) {
                return;
            }

            await manageServerService.updateCertificate(
                certificatesUtils.mapEditToDto(formData, certificate),
                certificate.HasTwoFactor && !formData.isRequire2FA
            );
            reset(formData);
            dispatch(certificatesActions.fetchData());
            dispatch(certificatesActions.editModalClosed());
        });
    };

    return (
        <Modal
            show
            size="lg"
            centered
            contentClassName="modal-border bulge-primary"
            backdrop="static"
            onHide={close}
            aria-labelledby={titleId}
        >
            <FormProvider {...form}>
                <form onSubmit={handleSubmit(handleEdit)}>
                    <Modal.Header className="pt-3 pb-0" onCloseClick={close}>
                        <Icon icon="certificate" addon="edit" size="lg" color="primary" margin="me-3" />
                        <h3 id={titleId} className="mb-0">
                            Edit client certificate
                        </h3>
                    </Modal.Header>
                    <Modal.Body>
                        <FormGroup>
                            <FormLabel>Name</FormLabel>
                            <Form.Control type="text" value={certificate.Name} disabled />
                        </FormGroup>
                        <CertificatesSecurityClearanceField />
                        <hr />
                        <CertificatesPermissionsField />
                        <hr />
                        <Certificates2FAField editingCert={certificate} />
                    </Modal.Body>
                    <Modal.Footer>
                        <Button variant="link" onClick={close} className="link-muted">
                            Cancel
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            variant="success"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                        >
                            <Icon icon="save" />
                            Save changes
                        </ButtonWithSpinner>
                    </Modal.Footer>
                </form>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    securityClearance: yup.string<SecurityClearance>(),
    databasePermissions: certificatesUtils.databasePermissionsSchema,
    isRequire2FA: yup.boolean(),
    isRegenerate2FAKey: yup.boolean(),
    authenticationKey: yup
        .string()
        .nullable()
        .when(["isRequire2FA", "isRegenerate2FAKey", "$certHasTwoFactor"], {
            is: (isRequire2FA: boolean, isRegenerate2FAKey: boolean, certHasTwoFactor: boolean) =>
                isRequire2FA && (!certHasTwoFactor || isRegenerate2FAKey),
            then: (schema) => schema.required(),
        }),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesEditFormData = FormData;
