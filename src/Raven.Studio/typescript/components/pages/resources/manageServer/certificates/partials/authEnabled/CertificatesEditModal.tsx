import { yupResolver } from "@hookform/resolvers/yup";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { Button, Form, FormGroup, Input, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import * as yup from "yup";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import CertificatesPermissionsField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesPermissionsField";
import Certificates2FAField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/Certificates2FAField";
import CertificatesSecurityClearanceField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesSecurityClearanceField";
import useConfirm from "components/common/ConfirmDialog";
import { useEventsCollector } from "components/hooks/useEventsCollector";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesEditModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const confirm = useConfirm();
    const { reportEvent } = useEventsCollector();

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

            if (formData.securityClearance === "ValidUser" && formData.databasePermissions.length === 0) {
                const isConfirmed = await confirm(certificatesUtils.noPrivilegesConfirmOptions);

                if (!isConfirmed) {
                    return;
                }
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
        <Modal isOpen wrapClassName="bs5" size="lg" centered contentClassName="modal-border bulge-success">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleEdit)}>
                    <ModalBody>
                        <div className="text-center">
                            <Icon icon="edit" className="fs-1" margin="m-0" />
                        </div>
                        <div className="position-absolute m-2 end-0 top-0">
                            <Button close onClick={() => dispatch(certificatesActions.editModalClosed())} />
                        </div>
                        <div className="text-center lead">Edit client certificate</div>
                        <FormGroup>
                            <Label>Name</Label>
                            <Input type="text" value={certificate.Name} disabled />
                        </FormGroup>
                        <CertificatesSecurityClearanceField />
                        <hr />
                        <CertificatesPermissionsField />
                        <Certificates2FAField editingCert={certificate} />
                    </ModalBody>
                    <ModalFooter>
                        <Button
                            color="link"
                            onClick={() => dispatch(certificatesActions.editModalClosed())}
                            className="link-muted"
                        >
                            Cancel
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            color="success"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                        >
                            Edit
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
    authenticationKey: yup.string().when("isRequire2FA", {
        is: true,
        then: (schema) => schema.required(),
    }),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesEditFormData = FormData;
