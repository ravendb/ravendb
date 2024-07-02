import { FormCheckbox, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import React, { useEffect, useRef, useState, ElementRef, PropsWithChildren } from "react";
import { FieldPath, FieldValues, Control } from "react-hook-form";
import { Row, Col, InputGroup, Button, Alert, UncontrolledPopover, PopoverBody } from "reactstrap";
import { useServices } from "components/hooks/useServices";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { QRCode } from "qrcodejs";
import copyToClipboard from "common/copyToClipboard";
import fileDownloader from "common/fileDownloader";
import { ConditionalPopover } from "components/common/ConditionalPopover";

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
        // eslint-disable-next-line react-hooks/exhaustive-deps
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
                                    title="Regenerate key"
                                    onClick={() => asyncGenerateSecret.execute(true)}
                                >
                                    <Icon icon="reset" margin="m-0" />
                                </Button>
                            )}
                        </InputGroup>
                        <ActionButton isEncryptionKeyValid={isEncryptionKeyValid}>
                            <Button
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
                                    block
                                    color="primary"
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
                                    block
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
                    <Alert color="warning" className="d-flex align-items-center mb-4">
                        <Icon icon="warning" margin="me-2" className="fs-2" />
                        <div>
                            Save the key in a safe place. It will not be available again. If you lose this key you could
                            lose access to your data
                        </div>
                    </Alert>
                </Col>
                <Col className="text-center">
                    <div ref={qrContainerRef} className="qrcode" />
                    <div className="text-center mt-1">
                        <small id="qrInfo" className="text-info">
                            <Icon icon="info" margin="m-0" /> what&apos;s this?
                        </small>
                    </div>
                    <UncontrolledPopover target="qrInfo" placement="top" trigger="hover">
                        <PopoverBody>
                            This is the encryption key in QR Code format for easy copying to a mobile device.
                        </PopoverBody>
                    </UncontrolledPopover>
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
                    <img class="qr_logo" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFoAAABaCAYAAAA4qEECAAAACXBIWXMAAAsTAAALEwEAmpwYAAAE7mlUWHRYTUw6Y29tLmFkb2JlLnhtcAAAAAAAPD94cGFja2V0IGJlZ2luPSLvu78iIGlkPSJXNU0wTXBDZWhpSHpyZVN6TlRjemtjOWQiPz4gPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyIgeDp4bXB0az0iQWRvYmUgWE1QIENvcmUgOS4wLWMwMDEgNzkuYzAyMDRiMiwgMjAyMy8wMi8wOS0wNjoyNjoxNCAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczpkYz0iaHR0cDovL3B1cmwub3JnL2RjL2VsZW1lbnRzLzEuMS8iIHhtbG5zOnBob3Rvc2hvcD0iaHR0cDovL25zLmFkb2JlLmNvbS9waG90b3Nob3AvMS4wLyIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0RXZ0PSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VFdmVudCMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIDI0LjUgKFdpbmRvd3MpIiB4bXA6Q3JlYXRlRGF0ZT0iMjAyMy0wNS0wOVQxMzozODo0OCswMjowMCIgeG1wOk1vZGlmeURhdGU9IjIwMjMtMDYtMTZUMTE6Mjc6NTcrMDI6MDAiIHhtcDpNZXRhZGF0YURhdGU9IjIwMjMtMDYtMTZUMTE6Mjc6NTcrMDI6MDAiIGRjOmZvcm1hdD0iaW1hZ2UvcG5nIiBwaG90b3Nob3A6Q29sb3JNb2RlPSIzIiB4bXBNTTpJbnN0YW5jZUlEPSJ4bXAuaWlkOjU1ZjZhOGFmLWE1MzYtYTc0YS05MTZlLTE5OWRlYWQ2NDRjMCIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDo1NWY2YThhZi1hNTM2LWE3NGEtOTE2ZS0xOTlkZWFkNjQ0YzAiIHhtcE1NOk9yaWdpbmFsRG9jdW1lbnRJRD0ieG1wLmRpZDo1NWY2YThhZi1hNTM2LWE3NGEtOTE2ZS0xOTlkZWFkNjQ0YzAiPiA8eG1wTU06SGlzdG9yeT4gPHJkZjpTZXE+IDxyZGY6bGkgc3RFdnQ6YWN0aW9uPSJjcmVhdGVkIiBzdEV2dDppbnN0YW5jZUlEPSJ4bXAuaWlkOjU1ZjZhOGFmLWE1MzYtYTc0YS05MTZlLTE5OWRlYWQ2NDRjMCIgc3RFdnQ6d2hlbj0iMjAyMy0wNS0wOVQxMzozODo0OCswMjowMCIgc3RFdnQ6c29mdHdhcmVBZ2VudD0iQWRvYmUgUGhvdG9zaG9wIDI0LjUgKFdpbmRvd3MpIi8+IDwvcmRmOlNlcT4gPC94bXBNTTpIaXN0b3J5PiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/PrvnqGkAAAsNSURBVHic7Z1/TBTpGce/O7u4IGL3QPyRE7KrHsbmqmt7BaomXXLW84BGNEav6p1cjNFKxDO5y52JnpoYo5dU6EVTSzAIobaXVOFabLFoUcnJ1UPd1L9QlDWKEfzBVIVll915+sfLCiz7Y2Z2ZnZBPslEmZn3ed7nO+++88477/uO7r1P8hGI+pK/B9yvATYA8wFYAFgBmAe2cDgGNjuAdgD/BXBR4byFZfnOXwfcb9A4H4GwAVgx8K81Ajvmgc3mt98OJvi3iILwPqIltA3ARgAFAEwq+7IObJ8A4AHUAqiExqJzGvoyASgG+1k3AiiE+iIHykPhgP/2gfxokgcthDYB2AcW2O8hrr7VAjNYftrB8mdSwuiin2QH3K+20PvAAtkL7UuvWExg+fMJHhG7C3ch9xfLR+wPKHT+4txI/dkQ+wL7Y8Kg4Da5RjiOQ/GaohEajhA6++0sbFu1Ra4fE4ASsDrQLNdIlDGD5b8GERSSbau2IPvtrFd/DxM6fVoaPt/wKThOVo2SAeAG2N19LFAAFo9NTmKO4/D5hk+RPi2N/e07YNAb8MVHnyHBGC/HbjGAVozeUhwMM1jpLpaTOMEYjy8++gwGvQH6OdkZAIBVOQV4950cOfYqAOySk3AU8T6Y6N9KTfhG0htwuvpYiU75UTLWL/uNVBsmsJ9WodSEo5RCsHhNUhOuW/YBE/qDpWukVhkmsJ+UVarTUY4VLG6TlEQJxnhwkxMn472sX0lJZ8LrKbIPK2SIzb37Tg4mxE0Qe74Jr7fIPqyQKDaX87NfSnFQgXGRfVjB9BCFThAE6HQ6MeeWYOy0kZWkFMDOcCfpiEiMsQKwJ6VxArMDwNehThAjdAaA/2D09FlEAx5AFoBbwU4Q86z9R4yLHA4TgG9CnRDuDUsxIujJIiLcu3dPbnLJ6HQ6GAwGGI1GTJo0CfHxsroT5GIF62bdF/AoEQXbMoiomyKgv7+fAERts1gstGvXLnI4HJGEIYVuYrqN0DOU0DWReo220L5Nr9fT5s2b6fHjx5GGJIYakiC0TQmPsSK0b0tNTaVz584pEVo4bCRS6EYlvMWa0ACI4zg6fvy4EuGFopH8NA3UvLOBPV5GjMfjQVxcHJKTk5GWlobJkyfD4/Ggs7MT9+/fR39/vxJuZFFeXo5Nmzap6SIHQ4c0+CtPCtTNPgRBoDt37gQ85nQ6qampib788ksym82al2yDwUBXrlxRKtRANFKIqsOspudgCIJAdXV1tGDBAk3Ftlgs1Nvbq2Zor1og/kKXqOk1HF6vl0pKSmjChAmaib1//341QyqhIEJ3q+lVLFevXqUpU6ZoInRKSgq9fPlSrVC6KcDNsAAadBxdv34dT58+xZw5c5Ceng69Xh/wvBs3bqC6uhpJSUlwuVzgeR5Pnz5Fa2srWltb4XK5FMtTVVUVPvzwQ8Xs+bESQO3Q0lyh1mUdSkVFxavSFBcXR3PnzqX8/Hy6cOGCaBsej4du3rxJR44coaysrIhL9cqVK1WMmCrIr+poV9ObD57naebMmcMCTUxMpPZ2+e6vXbtGubm5EVUfKtJOQ4TOUNOTP3fv3qWNGzeS1WqlvLw8+uGHHxSxe+rUKUpISJAltiAIiuQhCBk+oYvV9KIl3333HcXHx0sW2uPxqJmtYl9/9MJIa/xYYdGiRTh27JjkdCTuTZNccnytjhsYJS9dPR4Purq64Ha7AQAJCQlITU0dNl6QiJCZmYmWlhZJdoO1gBTA4RNa1csZCS6XC2fPnkVdXR2am5tx+/ZteL3eYecYDAbMnj0bVqsVS5YswbJly3D58mVs3rxZtB+v1yt3cKc4SOMboVh6enro4MGDsh9c0tPTY6XVQUQK9j0rSVNTE1ksFk2eDAHQihUrVI+JA5vTFzOUl5fDZrOhvb1dM5+rVq1S3wkR7VP9coqkrKxMs1Ls20wmk5p9Ha/QcvpbSM6fP4+tW7dq7nfnzp1ITExU3xHFQInu7u6madOmaV6azWYz9fT0aBJjTJToQ4cOobOzU1OfBoMBVVVVmDhxojYOKcol+vnz5zRp0iTNS3NZWZmmcUa9RJ85cwYvX77UzJ9Op0NpaamkhxkliPrqBmfOnNHMV3JyMiorK5GfH3jpDFWhKFYdLpeLJk6cqHo1odPpqLCwkB49ehStUMkA4JlmV9WP5uZm9Pb2qmY/LS0Na9aswZYtW/DWW2+p5kcMBrCVWqJCQ0ODarZLS0uxfft2dTuKJMABeBgt5+fOnVPN9q1bt2JGZGBwxD9p7fjJkyeYOnWqah3uHMehpaUFCxfGxjsNX6vDDo07/uvr60OKvHbtWkyfPn3YvgcPHuD06dOi7AuCgG3btuHKlStiJ0OpiUPToQZDWb9+fciWwrNnz0ak6e/vp/nz50tqcZw4cULr0AJRE5WXs16vl1JSUoKKk5ER/F1EU1OTJKFTU1MDXjSNKY7KcIPvv/8+pDgbNmwImX7dunWSxC4qKtIosqBkaD6Ahoho7969IYU5evRoyPQPHz6kpKQk0ULr9Xqy2+0aRTeCdiIaNv3tovQ6Xh5nz54NeTwzMzPk8RkzZmDPnj2i/Xm9XhQVFak9pCAYFwFgaIku0OLyPnr0iHQ6XdDSZzQaye12h7Xjdrtp3rx5kqqQyspKDSIcQQFFY9hueXl5SDGys7NF22poaJAk9NSpU4nneRWjG0E3DWjr/+h0UtEfTQBqa2tDHs/Kygp5fChLly7F6tWrRZ/f1dUlqcpRgJOv/kfDS7SqrY8XL16EHc1/6tQpSTbv3bsnaWCjxjfGoFMrFJ0s5E91dXVYIdra2iTbPXDggKQqZPHixWqPHiUKM1lI1QE1y5cvDynAlClTZNnt6+ujWbNmxdqNcdikTlUndA6lo6ODOI4LGXxubq5s+3V1dbF0Y2wkP02D9SPuD1nFy+DEiRMQBCHkOdnZgVeqFUNeXp6kV1Qq3xhH6uevPKlQV7vdbnrzzTfDlrL6+vqI/LS1tZHRaIz2jbGGJK5uEPEyEj6qqqpEBa5E58/u3bujeWPspiDLSIRb6qcYbDHriOjs7ITT6Qx5jl6vR1paWqSu4PV6cf/+fUlpZsyYAaPRGLFvsCpjX6ADYtZUakQEq9C8RtgRYorK+OJVysBDgcWrbgH4WKEMjVX2IoTIgPg1/mvBFtIbZySlCLPmHSB+gUEfNWBzxsdh1ILN9Q6LVKFNGF8E1ocdbJUZXszJUkeY8APG7RLTjTXskCAyIO87LDxeb7HtkCgyAHASqw4fPF5Pse2QITIAcLcftMl1yoM10E/KNTDKOAkWLy8nMdd47VKkGfgYbFnfscwORPgswV1oaYS73x1pRr4GMBfsw41jCQdYVRG2nRwO7nnPc/zr6vmIcwT2ZLQQY+fBphYsnotKGOMA4M8N38Dp6lPCHg+2DHsORm/pdoDlfyVk1sf+OF197MtCTpcTXsGLn85VbCyxA6x7VQf2cKPpQs4y4QEcBhPYoaThqn9WD7ajay/9DXc67ippH2B9sxawflpeaeMKwYPlzwIFvmfoz52Ou6i59O2g0B6vBwcrD6O3T/HJOzwGBd+B2KlSHGD58QnMK+3A6XLiYOVX8Hg9w58MOx4/xFd/OhL2JapMeLC7twWsDjwJ7Us5P+A3ZyAfX6uVB0EQcLj6d+h43AEAg19/8/GgqwP/63mOzB//XA3/PhxgX1I7DOASWLDxAKYHTyIbO4C/gH2h7rcDfh0q+BnGsdPH8e9rF1/9rRv/ALuyCIKAo3/9A/7RXD9s//8BcfrQxTtAmncAAAAASUVORK5CYII=">
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
