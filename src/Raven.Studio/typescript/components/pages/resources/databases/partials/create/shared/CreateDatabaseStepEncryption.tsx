import { FormCheckbox, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { CreateDatabaseRegularFormData } from "../regular/createDatabaseRegularValidation";
import { CreateDatabaseFromBackupFormData } from "../formBackup/createDatabaseFromBackupValidation";
import React, { useEffect, useRef, useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { Row, Col, InputGroup, Button, Alert, UncontrolledPopover, PopoverBody } from "reactstrap";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import { QRCode } from "qrcodejs";

const encryptionImg = require("Content/img/createDatabase/encryption.svg");

export default function CreateDatabaseStepEncryption() {
    const { databasesService } = useServices();

    const { control, trigger, setValue } = useFormContext<
        CreateDatabaseRegularFormData | CreateDatabaseFromBackupFormData
    >();
    const formValues = useWatch({ control });
    const { value: isDatabaseNameValid, setValue: setIsDatabaseNameValid } = useBoolean(false);

    // TODO trigger encryption key
    useEffect(() => {
        trigger("basicInfo.databaseName").then((isValid) => setIsDatabaseNameValid(isValid));
    }, [setIsDatabaseNameValid, trigger]);

    const asyncGenerateSecret = useAsyncCallback(async (isRegenerate) => {
        if (formValues.encryption.key && !isRegenerate) {
            return;
        }
        const key = await databasesService.generateSecret();
        setValue("encryption.key", key);
    });

    useEffect(() => {
        asyncGenerateSecret.execute(false);
        // only on mount
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // TODO copy/download/print buttons
    // TODO disable reason

    const qrContainerRef = useRef(null);

    const [qrCode, setQrCode] = useState<typeof QRCode>(null);

    // TODO set encryption key from restore point
    // useEffect(() => {
    //     if ("" in formValues.) {
    //     }
    // }, []);

    useEffect(() => {
        const generateQrCode = async () => {
            const isValid = await trigger("encryption.key");

            if (!isValid) {
                qrCode?.clear();
                return;
            }

            if (!qrCode) {
                setQrCode(
                    new QRCode(qrContainerRef.current, {
                        text: formValues.encryption.key,
                        width: 256,
                        height: 256,
                        colorDark: "#000000",
                        colorLight: "#ffffff",
                        correctLevel: QRCode.CorrectLevel.Q,
                    })
                );
            } else {
                qrCode.clear();
                qrCode.makeCode(formValues.encryption.key);
            }
        };

        generateQrCode();

        return () => {
            qrCode?.clear();
        };
    }, [formValues.encryption.key, qrCode, trigger]);

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={encryptionImg} alt="TODO" className="step-img" />b
            </div>

            <h2 className="text-center">Encryption at Rest</h2>

            <Row className="mt-4">
                <Col>
                    <div className="small-label mb-1">Key (Base64 Encoding)</div>
                    <InputGroup>
                        <FormInput type="text" control={control} name="encryption.key" />
                        <Button type="button" title="Regenerate key" onClick={() => asyncGenerateSecret.execute(true)}>
                            <Icon icon="reset" margin="m-0" />
                        </Button>
                        <Button type="button" title="Copy to clipboard" disabled={!isDatabaseNameValid}>
                            <Icon icon="copy-to-clipboard" margin="m-0" />
                        </Button>
                    </InputGroup>
                    <Row className="mt-2">
                        <Col>
                            <Button type="button" block color="primary" size="sm" disabled={!isDatabaseNameValid}>
                                <Icon icon="download" /> Download encryption key
                            </Button>
                        </Col>
                        <Col>
                            <Button type="button" block size="sm" disabled={!isDatabaseNameValid}>
                                <Icon icon="print" /> Print encryption key
                            </Button>
                        </Col>
                    </Row>
                    <Alert color="warning" className="d-flex align-items-center mt-2">
                        <Icon icon="warning" margin="me-2" className="fs-2" />
                        <div>
                            Save the key in a safe place. It will not be available again. If you lose this key you could
                            lose access to your data
                        </div>
                    </Alert>
                </Col>
                <Col lg="auto" className="text-center">
                    <div ref={qrContainerRef} className="qrcode" />
                    <div className="text-center mt-1">
                        <small id="qrInfo" className="text-info">
                            <Icon icon="info" margin="m-0" /> what&apos;s this?
                        </small>
                    </div>
                    <UncontrolledPopover target="qrInfo" placement="top" trigger="hover" container="PopoverContainer">
                        <PopoverBody>TODO: write info about qr code</PopoverBody>
                    </UncontrolledPopover>
                </Col>
            </Row>
            <div className="d-flex justify-content-center mt-3">
                <FormCheckbox control={control} name="encryption.isKeySaved" size="lg" color="primary">
                    <span className="lead ms-2">I have saved the encryption key</span>
                </FormCheckbox>
            </div>
        </div>
    );
}
