import classNames from "classnames";
import copyToClipboard from "common/copyToClipboard";
import { FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useServices } from "components/hooks/useServices";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { CertificateItem } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { QRCode } from "qrcodejs";
import { useRef, ElementRef, useState, useEffect } from "react";
import { flushSync } from "react-dom";
import { useAsync } from "react-async-hook";
import { useFormContext, useWatch } from "react-hook-form";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import InputGroup from "react-bootstrap/InputGroup";

export default function Certificates2FAField({ editingCert }: { editingCert?: CertificateItem }) {
    const { manageServerService } = useServices();

    const { control, setValue } = useFormContext<
        CertificatesGenerateFormData | CertificatesCloneFormData | CertificatesEditFormData
    >();

    const formValues = useWatch({ control });

    const qrContainerRef = useRef<ElementRef<"div">>(null);
    const authenticationKeyInputRef = useRef<HTMLInputElement>(null);
    const generateNewKeyButtonRef = useRef<HTMLButtonElement>(null);
    const [qrCode, setQrCode] = useState<typeof QRCode>(null);

    const hasCurrentKey = editingCert?.HasTwoFactor ?? false;
    const isRegenerate2FAKey = "isRegenerate2FAKey" in formValues && formValues.isRegenerate2FAKey;
    const isNewKeyNeeded = formValues.isRequire2FA && (!hasCurrentKey || isRegenerate2FAKey);
    const isCurrentKeyRemoved = hasCurrentKey && !formValues.isRequire2FA;
    const authenticationKeyInGroups = formValues.authenticationKey?.match(/.{1,4}/g)?.join(" ") ?? "";

    const pendingChange = (() => {
        if (isCurrentKeyRemoved) {
            return "Disabled on save";
        }
        if (hasCurrentKey && isRegenerate2FAKey) {
            return "New key on save";
        }
        if (editingCert && !hasCurrentKey && formValues.isRequire2FA) {
            return "Enabled on save";
        }
        return null;
    })();

    useAsync(async () => {
        if (isNewKeyNeeded) {
            const result = await manageServerService.generateTwoFactorSecret();
            setValue("authenticationKey", result.Secret);
        } else {
            setValue("authenticationKey", null);
        }
    }, [isNewKeyNeeded]);

    const toggleRegenerate2FAKey = (isRegenerate: boolean) => {
        flushSync(() => setValue("isRegenerate2FAKey", isRegenerate));
        (isRegenerate ? authenticationKeyInputRef : generateNewKeyButtonRef).current?.focus();
    };

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
                        width: 128,
                        height: 128,
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
        <div
            className={classNames("well rounded hstack gap-3 align-items-start p-3 mb-3", {
                "border border-warning": isCurrentKeyRemoved,
            })}
        >
            <div
                className={classNames("rounded p-2 lh-1 fs-5", {
                    "bg-faded-2fa": formValues.isRequire2FA,
                    "bg-faded-warning": isCurrentKeyRemoved,
                    "bg-faded-secondary": !formValues.isRequire2FA && !isCurrentKeyRemoved,
                })}
            >
                <Icon icon="shield" margin="m-0" />
            </div>
            <div className="vstack gap-3 flex-grow-1 min-w-0">
                <div className="hstack gap-3 align-items-start">
                    <div className="flex-grow-1">
                        <div className="hstack gap-1">
                            <span className="fw-semibold text-emphasis">Two-factor authentication</span>
                            {pendingChange && (
                                <Badge bg="faded-warning" pill>
                                    {pendingChange}
                                </Badge>
                            )}
                        </div>
                        <div className="small text-muted">
                            Require a code from an authenticator app in addition to the certificate.
                        </div>
                    </div>
                    <FormSwitch control={control} name="isRequire2FA" aria-label="Require two-factor authentication" />
                </div>
                {hasCurrentKey && formValues.isRequire2FA && !isRegenerate2FAKey && (
                    <>
                        <hr className="m-0" />
                        <div className="hstack gap-3">
                            <div className="flex-grow-1">
                                <div className="text-emphasis">Authentication key</div>
                                <div className="small text-muted">The current key stays in use.</div>
                            </div>
                            <Button
                                ref={generateNewKeyButtonRef}
                                variant="secondary"
                                onClick={() => toggleRegenerate2FAKey(true)}
                            >
                                <Icon icon="refresh" />
                                Generate new key
                            </Button>
                        </div>
                    </>
                )}
                <Collapse in={isNewKeyNeeded}>
                    <div>
                        <hr className="mt-0 mb-3" />
                        <div className="hstack gap-3 align-items-start">
                            <div ref={qrContainerRef} className="qrcode rounded-2 overflow-hidden flex-shrink-0" />
                            <div className="vstack gap-2 min-w-0">
                                <div>
                                    <div className="text-emphasis">Scan with an authenticator app</div>
                                    <div className="small text-muted">Or enter this key manually.</div>
                                </div>
                                <InputGroup>
                                    <Form.Control
                                        ref={authenticationKeyInputRef}
                                        readOnly
                                        value={authenticationKeyInGroups}
                                        placeholder="Generating key..."
                                        aria-label="Authentication key"
                                        className="font-monospace"
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
                                {hasCurrentKey && (
                                    <div className="hstack gap-1 flex-wrap">
                                        <span className="text-warning">
                                            <Icon icon="warning" />
                                            The current key stops working when you save.
                                        </span>
                                        <Button
                                            variant="link"
                                            size="sm"
                                            className="p-0"
                                            onClick={() => toggleRegenerate2FAKey(false)}
                                        >
                                            Keep current key
                                        </Button>
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                </Collapse>
            </div>
        </div>
    );
}
