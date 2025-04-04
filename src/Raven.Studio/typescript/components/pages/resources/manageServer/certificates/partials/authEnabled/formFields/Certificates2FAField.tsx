import copyToClipboard from "common/copyToClipboard";
import { FormSelect, FormInput, FormSwitch, FormGroup, FormLabel } from "components/common/Form";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { useServices } from "components/hooks/useServices";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { CertificatesRegenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesRegenerateModal";
import {
    CertificateItem,
    TwoFactorAction,
} from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { QRCode } from "qrcodejs";
import { useRef, ElementRef, useState, useEffect } from "react";
import { useAsync } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import InputGroup from "react-bootstrap/InputGroup";

export default function Certificates2FAField({ editingCert }: { editingCert?: CertificateItem }) {
    const { manageServerService } = useServices();

    const { control, setValue, watch } = useFormContext<
        | CertificatesGenerateFormData
        | CertificatesCloneFormData
        | CertificatesEditFormData
        | CertificatesRegenerateFormData
    >();

    const formValues = useWatch({ control });

    const qrContainerRef = useRef<ElementRef<"div">>(null);
    const [qrCode, setQrCode] = useState<typeof QRCode>(null);

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if ("twoFactorAction" in values && name === "twoFactorAction") {
                if (values.twoFactorAction === "leave" || values.twoFactorAction === "delete") {
                    setValue("isRequire2FA", false);
                }

                if (values.twoFactorAction === "update") {
                    setValue("isRequire2FA", true);
                }
            }
        });
        return () => unsubscribe();
    }, [setValue, watch]);

    useAsync(async () => {
        if (formValues.isRequire2FA) {
            const result = await manageServerService.generateTwoFactorSecret();
            setValue("authenticationKey", result.Secret);
        } else {
            setValue("authenticationKey", null);
        }
    }, [formValues.isRequire2FA]);

    const certName = "name" in formValues ? formValues.name : editingCert?.Name;

    useEffect(() => {
        const generateQrCode = async () => {
            if (!formValues.authenticationKey) {
                qrCode?.clear();
                return;
            }

            const encodedIssuer = encodeURIComponent(location.hostname);
            const encodedName = encodeURIComponent(certName ?? "Client Certificate");

            const uri = `otpauth://totp/${encodedIssuer}:${encodedName}?secret=${formValues.authenticationKey}&issuer=${encodedIssuer}`;

            if (!qrCode) {
                setQrCode(
                    new QRCode(qrContainerRef.current, {
                        text: uri,
                        width: 256,
                        height: 256,
                        colorDark: "#000000",
                        colorLight: "#ffffff",
                        correctLevel: QRCode.CorrectLevel.Q,
                    })
                );
            } else {
                qrCode.clear();
                qrCode.makeCode(uri);
            }
        };

        generateQrCode();

        return () => {
            qrCode?.clear();
        };
    }, [formValues.authenticationKey, certName, qrCode]);

    return (
        <>
            {editingCert?.HasTwoFactor ? (
                <FormGroup>
                    <FormLabel>Two-factor</FormLabel>
                    <FormSelect
                        control={control}
                        name="twoFactorAction"
                        placeholder="Select two-factor action"
                        options={twoFactorActionOptions}
                    />
                </FormGroup>
            ) : (
                <FormGroup>
                    <FormSwitch control={control} name="isRequire2FA">
                        Require two-factor authentication
                    </FormSwitch>
                </FormGroup>
            )}
            {"twoFactorAction" in formValues && formValues.twoFactorAction === "delete" && (
                <RichAlert variant="warning" className="mb-3">
                    Two-factor authentication (2FA) will be disabled for this certificate.
                </RichAlert>
            )}
            <Collapse in={formValues.isRequire2FA}>
                <div>
                    <FormGroup>
                        <FormLabel>Authentication Key</FormLabel>
                        <InputGroup>
                            <FormInput
                                type="text"
                                control={control}
                                name="authenticationKey"
                                disabled
                                className="border-top-right-radius-none border-bottom-right-radius-none"
                            />
                            <Button
                                onClick={() =>
                                    copyToClipboard.copy(
                                        formValues.authenticationKey,
                                        "Authentication key was copied to clipboard"
                                    )
                                }
                                variant="secondary"
                                title="Copy authentication key to clipboard"
                            >
                                <Icon icon="copy" margin="m-0" />
                            </Button>
                        </InputGroup>
                    </FormGroup>
                    <FormGroup>
                        <FormLabel>QR Code</FormLabel>
                        <br />
                        <div ref={qrContainerRef} className="qrcode rounded-2 overflow-hidden" />
                    </FormGroup>
                </div>
            </Collapse>
        </>
    );
}

const twoFactorActionOptions: SelectOption<TwoFactorAction>[] = [
    { value: "update", label: "Generate new 2FA authentication key" },
    { value: "delete", label: "Delete current 2FA authentication key and disable 2FA" },
    { value: "leave", label: "Do not modify current 2FA authentication key" },
];
