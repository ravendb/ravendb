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
import fetchSsoServerCertCommand = require("commands/auth/fetchSsoServerCertCommand");

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
    const [urlInput, setUrlInput] = useState("");
    const [urlFetching, setUrlFetching] = useState(false);
    const [urlError, setUrlError] = useState<string>(null);
    const [urlLoaded, setUrlLoaded] = useState<string>(null);

    const selectFile = (event: ChangeEvent<HTMLInputElement>) => {
        fileImporter.readAsDataURL(event.currentTarget, (dataUrl, fileName) => {
            const isFileSelected = fileName ? !!fileName.trim() : false;
            setImportedFileName(isFileSelected ? fileName.split(/(\\|\/)/g).pop() : null);
            setUrlLoaded(null);
            setValue("certificateAsBase64", dataUrl.substr(dataUrl.indexOf(",") + 1), { shouldValidate: true });
        });
    };

    const fetchFromUrl = async () => {
        if (!urlInput.trim()) {
            return;
        }
        setUrlFetching(true);
        setUrlError(null);
        setUrlLoaded(null);
        try {
            const result = await new fetchSsoServerCertCommand(urlInput.trim()).execute();
            setValue("certificateAsBase64", result.Base64, { shouldValidate: true });
            setImportedFileName(null);
            setUrlLoaded(urlInput.trim());
        } catch (e) {
            setUrlError(e?.responseJSON?.Error ?? "Failed to fetch certificate from URL. Use the file upload below.");
        } finally {
            setUrlFetching(false);
        }
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

    const showFilePicker = !urlLoaded;

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
                            <div className="hstack gap-1 mb-1">
                                <FormLabel className="mb-0 d-flex align-items-center gap-1">
                                    SSO server URL{" "}
                                    <ConditionalPopover
                                        conditions={{
                                            isActive: true,
                                            message: (
                                                <>
                                                    Enter the SSO server&apos;s URL (e.g.{" "}
                                                    <code>https://sso.example.com</code>). RavenDB will fetch the public
                                                    certificate from <code>/api/certificate</code> on that server — any
                                                    path is ignored. If unreachable, use the file upload below.
                                                </>
                                            ),
                                        }}
                                        popoverPlacement="right"
                                    >
                                        <Icon icon="info" margin="m-0" className="small" color="info" />
                                    </ConditionalPopover>
                                </FormLabel>
                            </div>
                            <InputGroup>
                                <Form.Control
                                    type="url"
                                    placeholder="https://sso.example.com"
                                    value={urlInput}
                                    onChange={(e) => {
                                        setUrlInput(e.target.value);
                                        setUrlError(null);
                                        setUrlLoaded(null);
                                    }}
                                    disabled={urlFetching}
                                />
                                <ButtonWithSpinner
                                    variant="secondary"
                                    onClick={fetchFromUrl}
                                    isSpinning={urlFetching}
                                    disabled={!urlInput.trim() || urlFetching}
                                >
                                    Fetch
                                </ButtonWithSpinner>
                            </InputGroup>
                            {urlError && <div className="text-danger small mt-1">{urlError}</div>}
                            {urlLoaded && (
                                <div className="text-success small mt-1">
                                    <Icon icon="check" margin="me-1" />
                                    Loaded from {urlLoaded}
                                </div>
                            )}
                        </FormGroup>
                        {showFilePicker && (
                            <FormGroup>
                                <div className="hstack gap-1">
                                    <FormLabel className="d-flex align-items-center gap-1">
                                        Or upload certificate file{" "}
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
                            </FormGroup>
                        )}
                        {formState.errors.certificateAsBase64 && (
                            <FormValidationMessage>
                                {formState.errors.certificateAsBase64.message}
                            </FormValidationMessage>
                        )}
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
    certificateAsBase64: yup.string().required("Certificate is required — use URL import or browse for a file"),
});

type FormData = yup.InferType<typeof schema>;
