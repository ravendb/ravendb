import { yupResolver } from "@hookform/resolvers/yup";
import genUtils from "common/generalUtils";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { useRef } from "react";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import { Button, Form, FormGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import * as yup from "yup";
import endpoints from "endpoints";
import notificationCenter from "common/notifications/notificationCenter";
import useConfirm from "components/common/ConfirmDialog";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import CertificatesPermissionsField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesPermissionsField";
import { ExpireTimeUnit } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import Certificates2FAField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/Certificates2FAField";
import CertificatesSecurityClearanceField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesSecurityClearanceField";
import CertificatesExpireField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesExpireField";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useEventsCollector } from "components/hooks/useEventsCollector";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesCloneModal() {
    const dispatch = useAppDispatch();
    const confirm = useConfirm();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();

    const downloadCertFormRef = useRef<HTMLFormElement>(null);

    const certificate = useAppSelector(certificatesSelectors.certificateToClone);

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            name: "",
            securityClearance: certificate.SecurityClearance,
            certificatePassphrase: "",
            ...certificatesUtils.mapExpireFromDro(certificate),
            databasePermissions: certificatesUtils.mapDatabasePermissionsFromDto(certificate),
            isRequire2FA: certificate.HasTwoFactor,
            authenticationKey: "",
        },
    });

    const { control, formState, handleSubmit, reset } = form;

    const formValues = useWatch({ control });

    const handleClone: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "clone");

            if (formData.securityClearance === "ValidUser" && formData.databasePermissions.length === 0) {
                const isConfirmed = await confirm(certificatesUtils.noPrivilegesConfirmOptions);

                if (!isConfirmed) {
                    return;
                }
            }

            const operationId = await databasesService.getNextOperationId(null);

            const url = `${endpoints.global.adminCertificates.adminCertificates}?operationId=${operationId}&raft-request-id=${genUtils.generateUUID()}`;
            downloadCertFormRef.current.setAttribute("action", url);
            downloadCertFormRef.current.submit();

            try {
                await notificationCenter.instance.monitorOperation(null, operationId);
                reset(formData);
                dispatch(certificatesActions.fetchData());
                dispatch(certificatesActions.cloneModalClosed());
            } catch {
                notificationCenter.instance.openDetailsForOperationById(null, operationId);
            }
        });
    };

    return (
        <Modal isOpen wrapClassName="bs5" size="lg" centered contentClassName="modal-border bulge-success">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleClone)}>
                    <ModalBody>
                        <div className="text-center">
                            <Icon icon="magic-wand" className="fs-1" margin="m-0" />
                        </div>
                        <div className="position-absolute m-2 end-0 top-0">
                            <Button close onClick={() => dispatch(certificatesActions.cloneModalClosed())} />
                        </div>
                        <div className="text-center lead">Clone client certificate</div>
                        <FormGroup>
                            <Label>Name</Label>
                            <FormInput type="text" control={control} name="name" />
                        </FormGroup>
                        <CertificatesSecurityClearanceField />
                        <FormGroup>
                            <Label>Certificate Passphrase</Label>
                            <FormInput type="password" control={control} name="certificatePassphrase" passwordPreview />
                        </FormGroup>
                        <CertificatesExpireField />
                        <hr />
                        <CertificatesPermissionsField />
                        <Certificates2FAField />
                    </ModalBody>
                    <ModalFooter>
                        <Button
                            color="link"
                            onClick={() => dispatch(certificatesActions.cloneModalClosed())}
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
                            Clone
                        </ButtonWithSpinner>
                    </ModalFooter>
                </Form>
            </FormProvider>

            {/* This form is used to download certificate */}
            <form ref={downloadCertFormRef} className="d-none" method="post">
                <input
                    name="Options"
                    value={JSON.stringify(certificatesUtils.mapGenerateToDto(formValues))}
                    onChange={() => {
                        // empty by design
                    }}
                />
            </form>
        </Modal>
    );
}

const schema = yup.object({
    name: yup.string().required(),
    securityClearance: yup.string<SecurityClearance>().required(),
    certificatePassphrase: yup.string(),
    expireIn: yup.number().nullable().positive().integer(),
    expireTimeUnits: yup.string<ExpireTimeUnit>(),
    databasePermissions: certificatesUtils.databasePermissionsSchema,
    isRequire2FA: yup.boolean(),
    authenticationKey: yup.string().when("isRequire2FA", {
        is: true,
        then: (schema) => schema.required(),
    }),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesGenerateFormData = FormData;
