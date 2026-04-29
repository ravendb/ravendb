import { yupResolver } from "@hookform/resolvers/yup";
import fileImporter from "common/fileImporter";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormGroup, FormInput, FormLabel, FormValidationMessage } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { useAppDispatch } from "components/store";
import { tryHandleSubmit } from "components/utils/common";
import { ChangeEvent, useState } from "react";
import { FormProvider, SubmitHandler, useForm } from "react-hook-form";
import Modal from "components/common/Modal";
import * as yup from "yup";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import InputGroup from "react-bootstrap/InputGroup";
import InputGroupText from "react-bootstrap/InputGroupText";

export default function CertificatesRegisterSsoServerModal() {
    const dispatch = useAppDispatch();
    const { manageServerService } = useServices();
    const { reportEvent } = useEventsCollector();

    const form = useForm<FormData>({
        resolver: yupResolver(schema),
        defaultValues: {
            name: "",
            certificateAsBase64: "",
        },
    });

    const { control, formState, handleSubmit, setValue, reset } = form;

    const [importedFileName, setImportedFileName] = useState<string>(null);

    const selectFile = (event: ChangeEvent<HTMLInputElement>) => {
        fileImporter.readAsDataURL(event.currentTarget, (dataUrl, fileName) => {
            const isFileSelected = fileName ? !!fileName.trim() : false;
            setImportedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
            setValue("certificateAsBase64", dataUrl.substr(dataUrl.indexOf(",") + 1), { shouldValidate: true });
        });
    };

    const handleRegister: SubmitHandler<FormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("certificates", "register-sso-server");

            await manageServerService.registerSsoServerCert({
                Name: formData.name,
                Certificate: formData.certificateAsBase64,
                Usage: "SsoServer" as const,
                SecurityClearance: "ValidUser",
                Permissions: {},
            });

            reset(formData);
            dispatch(certificatesActions.fetchData());
            dispatch(certificatesActions.isRegisterSsoServerModalOpenToggled());
        });
    };

    return (
        <Modal show size="lg" centered contentClassName="modal-border bulge-primary">
            <FormProvider {...form}>
                <Form onSubmit={handleSubmit(handleRegister)}>
                    <Modal.Header
                        className="vstack gap-4"
                        onCloseClick={() => dispatch(certificatesActions.isRegisterSsoServerModalOpenToggled())}
                    >
                        <div className="text-center">
                            <Icon icon="certificate" className="fs-1" color="primary" margin="m-0" />
                        </div>
                        <div className="text-center lead">Register SSO server certificate</div>
                    </Modal.Header>
                    <Modal.Body>
                        <FormGroup>
                            <FormLabel>Name</FormLabel>
                            <FormInput control={control} type="text" name="name" placeholder="e.g. My SSO Server" />
                        </FormGroup>
                        <FormGroup>
                            <div className="hstack gap-1">
                                <FormLabel className="d-flex align-items-center gap-1">
                                    Certificate File{" "}
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: true,
                                            message: (
                                                <>
                                                    Select the SSO server&apos;s public certificate file (
                                                    <code>.pfx</code> or <code>.cer</code>). Only the public key is
                                                    needed — no private key or passphrase required.
                                                </>
                                            ),
                                        }}
                                        popoverPlacement="right"
                                    >
                                        <Icon icon="info" margin="m-0" className="small" color="info" />
                                    </ConditionalPopover>
                                </FormLabel>
                            </div>
                            <input id="ssoServerFilePicker" type="file" onChange={selectFile} className="d-none" />
                            <InputGroup>
                                <span className="static-name form-control d-flex align-items-center">
                                    {importedFileName ? importedFileName : "Select file..."}
                                </span>
                                <InputGroupText>
                                    <label htmlFor="ssoServerFilePicker" className="cursor-pointer">
                                        <Icon icon="folder" />
                                        <span>Browse</span>
                                    </label>
                                </InputGroupText>
                            </InputGroup>
                            {formState.errors.certificateAsBase64 && (
                                <FormValidationMessage>
                                    {formState.errors.certificateAsBase64.message}
                                </FormValidationMessage>
                            )}
                        </FormGroup>
                    </Modal.Body>
                    <Modal.Footer>
                        <Button
                            variant="link"
                            onClick={() => dispatch(certificatesActions.isRegisterSsoServerModalOpenToggled())}
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
                            Register certificate
                        </ButtonWithSpinner>
                    </Modal.Footer>
                </Form>
            </FormProvider>
        </Modal>
    );
}

const schema = yup.object({
    name: yup.string().required(),
    certificateAsBase64: yup.string().required("Certificate file is required"),
});

type FormData = yup.InferType<typeof schema>;
