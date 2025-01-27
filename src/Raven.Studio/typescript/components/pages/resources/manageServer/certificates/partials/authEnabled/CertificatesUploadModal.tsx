import { yupResolver } from "@hookform/resolvers/yup";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import { Button, Form, FormGroup, Label, Modal, ModalBody, ModalFooter } from "reactstrap";
import * as yup from "yup";
import useConfirm from "components/common/ConfirmDialog";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import CertificatesPermissionsField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesPermissionsField";
import { ExpireTimeUnit } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { certificatesUtils } from "components/pages/resources/manageServer/certificates/utils/certificatesUtils";
import Certificates2FAField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/Certificates2FAField";
import CertificatesSecurityClearanceField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesSecurityClearanceField";
import CertificatesExpireField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesExpireField";
import CertificatesFileField from "components/pages/resources/manageServer/certificates/partials/authEnabled/formFields/CertificatesFileField";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesUploadModal() {
    const dispatch = useAppDispatch();
    const confirm = useConfirm();
    const { manageServerService } = useServices();

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
            if (formData.securityClearance === "ValidUser" && formData.databasePermissions.length === 0) {
                const isConfirmed = await confirm(certificatesUtils.noPrivilegesConfirmOptions);

                if (!isConfirmed) {
                    return;
                }
            }

            await manageServerService.uploadCertificate(certificatesUtils.mapUploadToDto(formData));
            reset(formData);
            dispatch(certificatesActions.fetchData());
            dispatch(certificatesActions.isUploadModalOpenToggled());
        });
    };

    return (
        <Modal isOpen wrapClassName="bs5" size="lg" centered contentClassName="modal-border bulge-success">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleUpload)}>
                    <ModalBody>
                        <div className="text-center">
                            <Icon icon="upload" className="fs-1" margin="m-0" />
                        </div>
                        <div className="position-absolute m-2 end-0 top-0">
                            <Button close onClick={() => dispatch(certificatesActions.isUploadModalOpenToggled())} />
                        </div>
                        <div className="text-center lead">Upload client certificate</div>
                        <FormGroup>
                            <Label>Name</Label>
                            <FormInput type="text" control={control} name="name" />
                        </FormGroup>
                        <CertificatesSecurityClearanceField />
                        <CertificatesFileField
                            infoPopoverBody={
                                <ul>
                                    <li>
                                        Select a <strong>.pfx file</strong> with single or multiple certificates.
                                    </li>
                                    <li>All certificates will be imported under a single name.</li>
                                </ul>
                            }
                        />
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
                            onClick={() => dispatch(certificatesActions.isUploadModalOpenToggled())}
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
                            Upload
                        </ButtonWithSpinner>
                    </ModalFooter>
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
    authenticationKey: yup.string().when("isRequire2FA", {
        is: true,
        then: (schema) => schema.required(),
    }),
});

type FormData = yup.InferType<typeof schema>;

export type CertificatesUploadFormData = FormData;
