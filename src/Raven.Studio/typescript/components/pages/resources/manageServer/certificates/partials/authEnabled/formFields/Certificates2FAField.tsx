import copyToClipboard from "common/copyToClipboard";
import { FormSelect, FormCheckbox, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import { SelectOption } from "components/common/select/Select";
import { useServices } from "components/hooks/useServices";
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
import { FormGroup, Label, Collapse, Button } from "reactstrap";

export default function Certificates2FAField({ editingCert }: { editingCert?: CertificateItem }) {
    const { manageServerService } = useServices();

    const { control, setValue, watch } = useFormContext<
        CertificatesGenerateFormData | CertificatesEditFormData | CertificatesRegenerateFormData
    >();

    const formValues = useWatch({ control });

    const qrContainerRef = useRef<ElementRef<"div">>(null);
    const [qrCode, setQrCode] = useState<typeof QRCode>(null);

    useEffect(() => {
        const { unsubscribe } = watch((values, { name }) => {
            if ("twoFactorAction" in values && name === "twoFactorAction") {
                if (values.twoFactorAction === "leave") {
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
                    <Label>Two-factor</Label>
                    <FormSelect
                        control={control}
                        name="twoFactorAction"
                        placeholder="Select two-factor action"
                        options={twoFactorActionOptions}
                    />
                </FormGroup>
            ) : (
                <FormGroup>
                    <FormCheckbox control={control} name="isRequire2FA">
                        Require two-factor authentication
                    </FormCheckbox>
                </FormGroup>
            )}
            {"twoFactorAction" in formValues && formValues.twoFactorAction === "delete" && (
                <RichAlert variant="warning">
                    Two-Factor authentication (2FA) will be disabled for this certificate.
                </RichAlert>
            )}
            <Collapse isOpen={formValues.isRequire2FA}>
                <FormGroup>
                    <Label>Authentication Key</Label>
                    <FormInput
                        type="text"
                        control={control}
                        name="authenticationKey"
                        addon={
                            <Button
                                onClick={() =>
                                    copyToClipboard.copy(
                                        formValues.authenticationKey,
                                        "Authentication Key was copied to clipboard."
                                    )
                                }
                                color="link"
                            >
                                <Icon icon="copy" margin="m-0" />
                            </Button>
                        }
                        disabled
                    />
                </FormGroup>
                <FormGroup>
                    <Label>QR Code:</Label>
                    <br />
                    <div ref={qrContainerRef} className="qrcode" />
                </FormGroup>
            </Collapse>
        </>
    );
}

const twoFactorActionOptions: SelectOption<TwoFactorAction>[] = [
    { value: "update", label: "Update existing authentication key" },
    { value: "delete", label: "Delete existing authentication key" },
    { value: "leave", label: "Leave existing authentication key" },
];
