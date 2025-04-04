import { yupResolver } from "@hookform/resolvers/yup";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { CloseButton, Form, Input, Modal, ModalBody, ModalFooter } from "reactstrap";
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

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesEditModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();
    const permissionsConfirm = useCertificatePermissionsConfirm();

    const certificate = useAppSelector(certificatesSelectors.certificateToEdit);

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            securityClearance: certificate.SecurityClearance,
            databasePermissions: certificatesUtils.mapDatabasePermissionsFromDto(certificate),
            twoFactorAction: null,
            isRequire2FA: false,
            authenticationKey: "",
        },
        context: {
            certHasTwoFactor: certificate.HasTwoFactor,
        },
    });

    const { control, formState, handleSubmit, reset } = form;

    const formValues = useWatch({ control });

    const handleEdit: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "edit");

            const isPermissionConfirmed = await permissionsConfirm(formData);
            if (!isPermissionConfirmed) {
                return;
            }

            await manageServerService.updateCertificate(
                certificatesUtils.mapEditToDto(formData, certificate),
                formValues.twoFactorAction === "delete"
            );
            reset(formData);
            dispatch(certificatesActions.fetchData());
            dispatch(certificatesActions.editModalClosed());
        });
    };

    return (
        <Modal isOpen wrapClassName="bs5" size="lg" centered contentClassName="modal-border bulge-primary">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleEdit)}>
                    <ModalBody>
                        <div className="text-center mb-3">
                            <Icon icon="certificate" addon="edit" className="fs-1" color="primary" margin="m-0" />
                        </div>
                        <div className="position-absolute m-2 end-0 top-0">
                            <CloseButton onClick={() => dispatch(certificatesActions.editModalClosed())} />
                        </div>
                        <div className="text-center lead mb-3">Edit client certificate</div>
                        <FormGroup>
                            <FormLabel>Name</FormLabel>
                            <Input type="text" value={certificate.Name} disabled />
                        </FormGroup>
                        <CertificatesSecurityClearanceField />
                        <hr />
                        <CertificatesPermissionsField />
                        <Certificates2FAField editingCert={certificate} />
                    </ModalBody>
                    <ModalFooter>
                        <Button
                            variant="link"
                            onClick={() => dispatch(certificatesActions.editModalClosed())}
                            className="link-muted"
                        >
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
                    </ModalFooter>
                </Form>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    securityClearance: yup.string<SecurityClearance>(),
    databasePermissions: certificatesUtils.databasePermissionsSchema,
    twoFactorAction: certificatesUtils.twoFactorActionSchema,
    isRequire2FA: yup.boolean(),
    authenticationKey: yup
        .string()
        .nullable()
        .when("isRequire2FA", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesEditFormData = FormData;
