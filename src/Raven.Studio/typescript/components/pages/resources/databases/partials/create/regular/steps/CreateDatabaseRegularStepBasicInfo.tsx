import { FormInput, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { LicenseRestrictions } from "components/common/LicenseRestrictions";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CreateDatabaseRegularFormData } from "../createDatabaseRegularValidation";
import { useAppSelector } from "components/store";
import React from "react";
import { useFormContext } from "react-hook-form";
import { Col, FormGroup, PopoverBody, Row, UncontrolledPopover } from "reactstrap";
import { Switch } from "components/common/Checkbox";

const newDatabaseImg = require("Content/img/createDatabase/new-database.svg");

export default function CreateDatabaseRegularStepBasicInfo() {
    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    const { control } = useFormContext<CreateDatabaseRegularFormData>();

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={newDatabaseImg} alt="" className="step-img" />
            </div>
            <h2 className="text-center mb-4">Create new database</h2>
            <Row>
                <Col lg={{ offset: 2, size: 8 }} className="text-center">
                    <FormGroup floating>
                        {/* <Label for="DbName">Database Name</Label> */}
                        <FormInput
                            type="text"
                            control={control}
                            name="basicInfo.databaseName"
                            placeholder="Database Name"
                            id="DbName"
                        />
                    </FormGroup>
                    <div className="d-flex align-items-center justify-content-center mt-2">
                        {hasEncryption ? (
                            <LicenseRestrictions
                                isAvailable={isSecureServer}
                                message={
                                    <>
                                        <p className="lead text-warning">
                                            <Icon icon="unsecure" margin="m-0" /> Authentication is off
                                        </p>
                                        <p>
                                            <strong>Encryption at Rest</strong> is only possible when authentication is
                                            enabled and a server certificate has been defined.
                                        </p>
                                        <p>
                                            For more information go to the <a href="#">certificates page</a>
                                        </p>
                                    </>
                                }
                            >
                                <FormSwitch
                                    control={control}
                                    name="basicInfo.isEncrypted"
                                    size="lg"
                                    color="primary"
                                    disabled={!isSecureServer}
                                >
                                    <span className="lead">
                                        <Icon icon="encryption" />
                                        Encrypt at Rest
                                    </span>
                                </FormSwitch>
                            </LicenseRestrictions>
                        ) : (
                            <LicenseRestrictions
                                isAvailable={false}
                                featureName={
                                    <strong className="text-primary">
                                        <Icon icon="storage" addon="encryption" margin="m-0" /> Storage encryption
                                    </strong>
                                }
                            >
                                <Switch
                                    selected={false}
                                    disabled={true}
                                    toggleSelection={null}
                                    size="lg"
                                    color="primary"
                                >
                                    <span className="lead">
                                        <Icon icon="encryption" />
                                        Encrypt at Rest
                                    </span>
                                </Switch>
                            </LicenseRestrictions>
                        )}

                        <Icon icon="info" color="info" id="encryptionInfo" margin="ms-1" />
                    </div>
                    <UncontrolledPopover
                        target="encryptionInfo"
                        placement="top"
                        trigger="hover"
                        container="PopoverContainer"
                    >
                        <PopoverBody>
                            Data will be encrypted at the storage engine layer, using <code>XChaCha20-Poly1305</code>{" "}
                            authenticated encryption algorithm.
                        </PopoverBody>
                    </UncontrolledPopover>
                </Col>
            </Row>
        </div>
    );
}
