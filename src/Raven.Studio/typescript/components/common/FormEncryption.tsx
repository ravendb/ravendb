import { FormCheckbox, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useEffect, useRef, useState, ElementRef, PropsWithChildren } from "react";
import { FieldPath, FieldValues, Control } from "react-hook-form";
import InputGroup from "react-bootstrap/InputGroup";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { QRCode } from "qrcodejs";
import copyToClipboard from "common/copyToClipboard";
import fileDownloader from "common/fileDownloader";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import RichAlert from "components/common/RichAlert";
import PopoverWithHoverWrapper from "./PopoverWithHoverWrapper";

const encryptionImg = require("Content/img/createDatabase/encryption.svg");
const qrLogo = require("Content/img/qr_logo.png");

export interface FormEncryptionProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> {
    control: Control<TFieldValues>;
    encryptionKeyFieldName: TName;
    encryptionKey: string;
    isSavedFieldName: TName;
    keyText: string;
    fileName: string;
    setEncryptionKey: (value: string) => void;
    triggerEncryptionKey: () => Promise<boolean>;
    isReadOnly?: boolean;
}

export default function FormEncryption<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    encryptionKeyFieldName,
    encryptionKey,
    isSavedFieldName,
    keyText,
    fileName,
    setEncryptionKey,
    triggerEncryptionKey,
    isReadOnly,
}: FormEncryptionProps<TFieldValues, TName>) {
    const { databasesService } = useServices();

    const asyncGenerateSecret = useAsyncCallback(async (isRegenerate) => {
        if (encryptionKey && !isRegenerate) {
            return;
        }
        const generatedKey = await databasesService.generateSecret();
        setEncryptionKey(generatedKey);
    });

    const { result: isEncryptionKeyValid } = useAsync(async () => {
        return await triggerEncryptionKey();
    }, [encryptionKey]);

    const qrContainerRef = useRef<ElementRef<"div">>(null);
    const [qrCode, setQrCode] = useState<typeof QRCode>(null);

    // Get initial encryption key
    useEffect(() => {
        asyncGenerateSecret.execute(false);
        // only on mount
    }, []);

    useEffect(() => {
        const generateQrCode = async () => {
            if (!isEncryptionKeyValid) {
                qrCode?.clear();
                return;
            }

            if (!qrCode) {
                setQrCode(
                    new QRCode(qrContainerRef.current, {
                        text: encryptionKey,
                        width: 256,
                        height: 256,
                        colorDark: "#000000",
                        colorLight: "#ffffff",
                        correctLevel: QRCode.CorrectLevel.Q,
                    })
                );
            } else {
                qrCode.clear();
                qrCode.makeCode(encryptionKey);
            }
        };

        generateQrCode();

        return () => {
            qrCode?.clear();
        };
    }, [encryptionKey, isEncryptionKeyValid, qrCode]);

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={encryptionImg} alt="Encryption" className="step-img" />
            </div>
            <h2 className="text-center">Encryption at Rest</h2>
            <Row className="mt-4">
                <Col xs="12" sm="8">
                    <div className="small-label mb-1">Key (Base64 Encoding)</div>
                    <div className="d-flex">
                        <InputGroup>
                            <FormInput
                                type="text"
                                control={control}
                                name={encryptionKeyFieldName}
                                readOnly={isReadOnly}
                            />
                            {!isReadOnly && (
                                <Button
                                    type="button"
                                    variant="secondary"
                                    title="Regenerate key"
                                    onClick={() => asyncGenerateSecret.execute(true)}
                                >
                                    <Icon icon="reset" margin="m-0" />
                                </Button>
                            )}
                        </InputGroup>
                        <ActionButton isEncryptionKeyValid={isEncryptionKeyValid}>
                            <Button
                                variant="secondary"
                                type="button"
                                title="Copy to clipboard"
                                onClick={() =>
                                    copyToClipboard.copy(keyText, "Encryption key data was copied to clipboard")
                                }
                                disabled={!isEncryptionKeyValid}
                                className="ms-1"
                            >
                                <Icon icon="copy-to-clipboard" margin="m-0" />
                            </Button>
                        </ActionButton>
                    </div>

                    <Row className="mt-2">
                        <Col lg="6">
                            <ActionButton isEncryptionKeyValid={isEncryptionKeyValid}>
                                <Button
                                    type="button"
                                    variant="primary"
                                    size="sm"
                                    onClick={() => fileDownloader.downloadAsTxt(keyText, fileName)}
                                    disabled={!isEncryptionKeyValid}
                                    className="mb-2"
                                >
                                    <Icon icon="download" /> Download encryption key
                                </Button>
                            </ActionButton>
                        </Col>
                        <Col lg="6">
                            <ActionButton isEncryptionKeyValid={isEncryptionKeyValid}>
                                <Button
                                    type="button"
                                    variant="secondary"
                                    size="sm"
                                    onClick={() =>
                                        printEncryptionKey(keyText, fileName, qrContainerRef.current.innerHTML)
                                    }
                                    disabled={!isEncryptionKeyValid}
                                    className="mb-2"
                                >
                                    <Icon icon="print" /> Print encryption key
                                </Button>
                            </ActionButton>
                        </Col>
                    </Row>
                    <RichAlert variant="warning" className="mb-4">
                        <div>
                            Save the key in a safe place. It will not be available again. If you lose this key you could
                            lose access to your data
                        </div>
                    </RichAlert>
                </Col>
                <Col className="text-center">
                    <div ref={qrContainerRef} className="qrcode" />
                    <div className="text-center mt-1">
                        <PopoverWithHoverWrapper message="This is the encryption key in QR Code format for easy copying to a mobile device.">
                            <small className="text-info">
                                <Icon icon="info" margin="m-0" /> what&apos;s this?
                            </small>
                        </PopoverWithHoverWrapper>
                    </div>
                </Col>
            </Row>
            <div className="d-flex justify-content-center mt-3">
                <FormCheckbox control={control} name={isSavedFieldName} size="lg" color="primary">
                    <span className="lead ms-2">I have saved the encryption key</span>
                </FormCheckbox>
            </div>
        </div>
    );
}

const printEncryptionKey = (keyText: string, fileName: string, qrCodeHtml: string) => {
    const text = keyText.replace(/\r\n/g, "<br/>");

    const html = `
        <html>
            <head>
                <title>${fileName}</title>
                <style>
                    body {
                        text-align: center;
                        font-family: "Helvetica Neue", Helvetica, Arial, sans-serif;
                    }
                    #encryption_qrcode {
                        position: relative;
                        display: inline-block;
                    }

                    h4 {
                        font-weight: normal;
                    }
                
                    .qr_logo {
                        position: absolute;
                        left: 50%;
                        top: 50%;
                        -moz-transform: translateX(-50%) translateY(-50%);
                        -webkit-transform: translateX(-50%) translateY(-50%);
                        -o-transform: translateX(-50%) translateY(-50%);
                        -ms-transform: translateX(-50%) translateY(-50%);
                        transform: translateX(-50%) translateY(-50%);
                    }
                </style>
            </head>
            <body>
                <h4>${text}</h4>
                <br />
                <div id="encryption_qrcode">
                    <img class="qr_logo" src=${qrLogo}>
                    ${qrCodeHtml}
                </div>
            </body>                
        </html>
    `;

    const printWindow = window.open();
    printWindow.document.write(html);
    printWindow.document.close();

    printWindow.focus();
    setTimeout(() => {
        printWindow.print();
        printWindow.close();
    }, 50);
};

function ActionButton({ children, isEncryptionKeyValid }: PropsWithChildren<{ isEncryptionKeyValid: boolean }>) {
    return (
        <ConditionalPopover
            conditions={{
                isActive: !isEncryptionKeyValid,
                message: "Encryption key is not valid",
            }}
            popoverPlacement="top"
        >
            {children}
        </ConditionalPopover>
    );
}
