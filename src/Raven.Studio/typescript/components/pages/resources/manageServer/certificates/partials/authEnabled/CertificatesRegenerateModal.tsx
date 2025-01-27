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
import Certificates2FAField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/Certificates2FAField";
import CertificatesExpireField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesExpireField";
import { ExpireTimeUnit } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import { useEventsCollector } from "components/hooks/useEventsCollector";

export default function CertificatesRegenerateModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const certificate = useAppSelector(certificatesSelectors.certificateToRegenerate);
    const { reportEvent } = useEventsCollector();

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            expireIn: null,
            expireTimeUnits: "months",
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

    const handleRegenerate: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "re-generate");
            await manageServerService.updateCertificate(
                certificatesUtils.mapRegenerateToDto(formData, certificate),
                formValues.twoFactorAction === "delete"
            );
            reset(formData);
            dispatch(certificatesActions.fetchData());
            dispatch(certificatesActions.regenerateModalClosed());
        });
    };

    return (
        <Modal isOpen wrapClassName="bs5" size="lg" centered contentClassName="modal-border bulge-success">
            <FormProvider {...form}>
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
                            <Input type="text" value={certificate.Name} disabled />
                        </FormGroup>
                        <CertificatesExpireField />
                        <Certificates2FAField editingCert={certificate} />
                    </ModalBody>
                    <ModalFooter>
                        <Button
                            color="link"
                            onClick={() => dispatch(certificatesActions.regenerateModalClosed())}
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
                            Generate
                        </ButtonWithSpinner>
                    </ModalFooter>
                </Form>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    expireIn: yup.number().nullable().positive().integer(),
    expireTimeUnits: yup.string<ExpireTimeUnit>(),
    twoFactorAction: certificatesUtils.twoFactorActionSchema,
    isRequire2FA: yup.boolean(),
    authenticationKey: yup.string().when("isRequire2FA", {
        is: true,
        then: (schema) => schema.required(),
    }),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesRegenerateFormData = FormData;
