/* eslint-disable react-compiler/react-compiler */
import { yupResolver } from "@hookform/resolvers/yup";
import genUtils from "common/generalUtils";
import { FormInput, FormGroup, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch, useAppSelector } from "components/store";
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
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import useCertificatePermissionsConfirm from "components/pages/resources/manageServer/certificates/utils/useCertificatePermissionsConfirm";
import Modal from "components/common/Modal";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesCloneModal() {
    const dispatch = useAppDispatch();
    const { databasesService } = useServices();
    const { reportEvent } = useEventsCollector();
    const permissionsConfirm = useCertificatePermissionsConfirm();

    const downloadCertFormRef = useRef<HTMLFormElement>(null);

    const certificate = useAppSelector(certificatesSelectors.certificateToClone);

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            name: certificate.Name,
            securityClearance: certificate.SecurityClearance,
            certificatePassphrase: "",
            ...certificatesUtils.mapExpireFromDto(certificate),
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
                dispatch(certificatesActions.cloneModalClosed());
            } catch {
                notificationCenter.instance.openDetailsForOperationById(null, operationId);
            }
        });
    };

    return (
        <Modal show size="lg" centered contentClassName="modal-border bulge-primary">
            <FormProvider {...form}>
                <form onSubmit={handleSubmit(handleClone)}>
                    <Modal.Header
                        className="vstack gap-4"
                        onCloseClick={() => dispatch(certificatesActions.cloneModalClosed())}
                    >
                        <div className="text-center">
                            <Icon icon="copy" className="fs-1" color="primary" margin="m-0" />
                        </div>
                        <div className="text-center lead">Clone client certificate</div>
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
                            onClick={() => dispatch(certificatesActions.cloneModalClosed())}
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
                            Clone certificate
                        </ButtonWithSpinner>
                    </Modal.Footer>
                </form>
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

export type CertificatesCloneFormData = FormData;
