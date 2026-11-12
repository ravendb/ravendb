import { yupResolver } from "@hookform/resolvers/yup";
import { FormGroup, FormInput, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import Modal from "components/common/Modal";
import * as yup from "yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import CertificatesPermissionsField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesPermissionsField";
import { ExpireTimeUnit } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import Certificates2FAField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/Certificates2FAField";
import CertificatesSecurityClearanceField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesSecurityClearanceField";
import CertificatesExpireField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesExpireField";
import CertificatesFileField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesFileField";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import useCertificatePermissionsConfirm from "components/pages/resources/manageServer/certificates/utils/useCertificatePermissionsConfirm";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import useUniqueId from "components/hooks/useUniqueId";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesUploadModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();
    const permissionsConfirm = useCertificatePermissionsConfirm();
    const titleId = useUniqueId("upload-certificate-title");

    const close = () => dispatch(certificatesActions.isUploadModalOpenToggled());

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            name: "",
            securityClearance: "ValidUser",
            certificateAsBase64: "",
            certificatePassphrase: "",
            expireIn: null,
            expireTimeUnits: "months",
            databasePermissions: [],
            isRequire2FA: false,
            authenticationKey: "",
        },
    });

    const { control, formState, handleSubmit, reset } = form;

    const handleUpload: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "upload");

            const isPermissionConfirmed = await permissionsConfirm(formData);
            if (!isPermissionConfirmed) {
                return;
            }

            await manageServerService.uploadCertificate(certificatesUtils.mapUploadToDto(formData));
            reset(formData);
            dispatch(certificatesActions.fetchData());
            dispatch(certificatesActions.isUploadModalOpenToggled());
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
                <Form onSubmit={handleSubmit(handleUpload)}>
                    <Modal.Header className="pt-3 pb-0" onCloseClick={close}>
                        <Icon icon="upload" size="lg" color="primary" margin="me-3" />
                        <h3 id={titleId} className="mb-0">
                            Upload client certificate
                        </h3>
                    </Modal.Header>
                    <Modal.Body>
                        <FormGroup>
                            <FormLabel>Name</FormLabel>
                            <FormInput control={control} type="text" name="name" />
                        </FormGroup>
                        <CertificatesSecurityClearanceField />
                        <CertificatesFileField
                            infoPopoverBody={
                                <div>
                                    Select a <strong>.pfx file</strong> with single or multiple certificates. All
                                    certificates will be imported under a single certificate entry.
                                </div>
                            }
                        />
                        <FormGroup>
                            <FormLabel className="form-label">Certificate Passphrase</FormLabel>
                            <FormInput type="password" control={control} name="certificatePassphrase" passwordPreview />
                        </FormGroup>
                        <CertificatesExpireField />
                        <hr />
                        <CertificatesPermissionsField />
                        <hr />
                        <Certificates2FAField />
                    </Modal.Body>
                    <Modal.Footer>
                        <Button variant="link" onClick={close} className="link-muted">
                            Cancel
                        </Button>
                        <ButtonWithSpinner
                            type="submit"
                            variant="primary"
                            className="rounded-pill"
                            isSpinning={formState.isSubmitting}
                        >
                            Upload certificate
                        </ButtonWithSpinner>
                    </Modal.Footer>
                </Form>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    name: yup.string().required(),
    securityClearance: yup.string<SecurityClearance>().required(),
    certificateAsBase64: yup.string().required(),
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

export type CertificatesUploadFormData = FormData;
