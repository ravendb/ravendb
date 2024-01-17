import { FormCheckbox } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { CreateDatabaseRegularFormData } from "../createDatabaseRegularValidation";
import React from "react";
import { useFormContext } from "react-hook-form";
import { Row, Col, InputGroup, Input, Button, Alert, UncontrolledPopover, PopoverBody } from "reactstrap";

const encryptionImg = require("Content/img/createDatabase/encryption.svg");
const qrImg = require("Content/img/createDatabase/qr.jpg");

export default function CreateDatabaseRegularStepEncryption() {
    const { control } = useFormContext<CreateDatabaseRegularFormData>();

    // TODO copy/download/print buttons

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={encryptionImg} alt="" className="step-img" />
            </div>

            <h2 className="text-center">Encryption at Rest</h2>

            <Row className="mt-4">
                <Col>
                    <div className="small-label mb-1">Key (Base64 Encoding)</div>
                    <InputGroup>
                        <Input value="13a5f83gy71ws032nm69" readOnly />
                        <Button type="button" title="Copy to clipboard">
                            <Icon icon="copy-to-clipboard" margin="m-0" />
                        </Button>
                    </InputGroup>
                    <Row className="mt-2">
                        <Col>
                            <Button type="button" block color="primary" size="sm">
                                <Icon icon="download" /> Download encryption key
                            </Button>
                        </Col>
                        <Col>
                            <Button type="button" block size="sm">
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
                    <img src={qrImg} alt="" />
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
                <FormCheckbox control={control} name="isEncryptionKeySaved" size="lg" color="primary">
                    <span className="lead ms-2">I have saved the encryption key</span>
                </FormCheckbox>
            </div>
        </div>
    );
}
