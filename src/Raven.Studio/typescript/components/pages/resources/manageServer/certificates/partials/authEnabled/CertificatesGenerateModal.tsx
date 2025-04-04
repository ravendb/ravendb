/* eslint-disable react-compiler/react-compiler */
import { yupResolver } from "@hookform/resolvers/yup";
import genUtils from "common/generalUtils";
import { FormInput, FormGroup, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { useRef } from "react";
import { FormProvider, SubmitHandler, useForm, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import * as yup from "yup";
import endpoints from "endpoints";
import notificationCenter from "common/notifications/notificationCenter";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import CertificatesPermissionsField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesPermissionsField";
import { ExpireTimeUnit } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import Certificates2FAField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/Certificates2FAField";
import CertificatesSecurityClearanceField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesSecurityClearanceField";
import CertificatesExpireField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesExpireField";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import useCertificatePermissionsConfirm from "components/pages/resources/manageServer/certificates/utils/useCertificatePermissionsConfirm";
import Modal from "components/common/Modal";
import Form from "react-bootstrap/Form";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesGenerateModal() {
    const dispatch = useAppDispatch();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const permissionsConfirm = useCertificatePermissionsConfirm();

    const downloadCertFormRef = useRef<HTMLFormElement>(null);

    const form = useForm<FormData>({
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

    const { control, formState, handleSubmit, reset } = form;

    const formValues = useWatch({ control });

    const handleGenerate: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "generate");

            const isPermissionConfirmed = await permissionsConfirm(formData);
            if (!isPermissionConfirmed) {
                return;
            }

            const operationId = await databasesService.getNextOperationId(null);

            const url = `${endpoints.global.adminCertificates.adminCertificates}?operationId=${operationId}&raft-request-id=${genUtils.generateUUID()}`;
            downloadCertFormRef.current.setAttribute("action", url);
            downloadCertFormRef.current.submit();

            try {
                await notificationCenter.instance.monitorOperation(null, operationId);
                reset(formData);
                dispatch(certificatesActions.fetchData());
                dispatch(certificatesActions.isGenerateModalOpenToggled());
            } catch {
                notificationCenter.instance.openDetailsForOperationById(null, operationId);
            }
        });
    };

    return (
        <Modal show size="lg" centered contentClassName="modal-border bulge-primary">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleGenerate)}>
                    <Modal.Header
                        className="vstack gap-4"
                        onCloseClick={() => dispatch(certificatesActions.isGenerateModalOpenToggled())}
                    >
                        <div className="text-center">
                            <Icon icon="certificate" addon="plus" color="primary" className="fs-1" margin="m-0" />
                        </div>
                        <div className="text-center lead">Generate client certificate</div>
                    </Modal.Header>
                    <Modal.Body>
                        <FormGroup>
                            <FormLabel>Name</FormLabel>
                            <FormInput type="text" control={control} name="name" />
                        </FormGroup>
                        <CertificatesSecurityClearanceField />
                        <FormGroup>
                            <FormLabel>Certificate Passphrase</FormLabel>
                            <FormInput type="password" control={control} name="certificatePassphrase" passwordPreview />
                        </FormGroup>
                        <CertificatesExpireField />
                        <hr />
                        <CertificatesPermissionsField />
                        <Certificates2FAField />
                    </Modal.Body>
                    <Modal.Footer>
                        <Button
                            variant="link"
                            onClick={() => dispatch(certificatesActions.isGenerateModalOpenToggled())}
                            className="link-muted"
                        >
                            Cancel
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            variant="primary"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                        >
                            Generate certificate
                        </ButtonWithSpinner>
                    </Modal.Footer>
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
    authenticationKey: yup
        .string()
        .nullable()
        .when("isRequire2FA", {
            is: true,
            then: (schema) => schema.required(),
        }),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesGenerateFormData = FormData;
