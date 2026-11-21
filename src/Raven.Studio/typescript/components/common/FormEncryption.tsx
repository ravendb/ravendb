import { FormCheckbox, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { useEffect, useRef, useState, ElementRef, PropsWithChildren } from "react";
import { FieldPath, FieldValues, Control } from "react-hook-form";
import InputGroup from "react-bootstrap/InputGroup";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Button from "react-bootstrap/Button";
import { useAsync } from "react-async-hook";
import { useEncryptionKeyGenerator } from "components/hooks/useEncryptionKeyGenerator";
import { QRCode } from "qrcodejs";
import copyToClipboard from "common/copyToClipboard";
import fileDownloader from "common/fileDownloader";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import RichAlert from "components/common/RichAlert";
import PopoverWithHoverWrapper from "./PopoverWithHoverWrapper";
import { encryptionKeyQrCodeUtils } from "components/utils/encryptionKeyQrCodeUtils";

const encryptionImg = require("Content/img/createDatabase/encryption.svg");

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
    const { isGenerating, regenerate } = useEncryptionKeyGenerator(encryptionKey, setEncryptionKey);

    const { result: isEncryptionKeyValid } = useAsync(async () => {
        return await triggerEncryptionKey();
    }, [encryptionKey]);

    const qrContainerRef = useRef<ElementRef<"div">>(null);
    const [qrCode, setQrCode] = useState<typeof QRCode>(null);

    useEffect(() => {
        const generateQrCode = async () => {
            if (!isEncryptionKeyValid) {
                qrCode?.clear();
                return;
            }

            if (!qrCode) {
                setQrCode(encryptionKeyQrCodeUtils.renderQrCode(qrContainerRef.current, encryptionKey, 256));
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
                                    onClick={regenerate}
                                    disabled={isGenerating}
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
                                        encryptionKeyQrCodeUtils.printEncryptionKey(
                                            keyText,
                                            fileName,
                                            qrContainerRef.current.innerHTML
                                        )
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
