import { FormInput, FormSwitch } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CreateDatabaseRegularFormData as FormData } from "../createDatabaseRegularValidation";
import { useAppSelector } from "components/store";
import React from "react";
import { useFormContext } from "react-hook-form";
import { Col, PopoverBody, Row, UncontrolledPopover } from "reactstrap";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import AuthenticationOffMessage from "components/pages/resources/databases/partials/create/shared/AuthenticationOffMessage";
import EncryptionUnavailableMessage from "components/pages/resources/databases/partials/create/shared/EncryptionUnavailableMessage";

const newDatabaseImg = require("Content/img/createDatabase/new-database.svg");

export default function CreateDatabaseRegularStepBasicInfo() {
    const { control } = useFormContext<FormData>();

    return (
        <div>
            <div className="d-flex justify-content-center">
                <img src={newDatabaseImg} alt="" className="step-img" />
            </div>
            <h2 className="text-center mb-4">Create new database</h2>
            <Row>
                <Col lg={{ offset: 2, size: 8 }} className="text-center">
                    <FormInput
                        type="text"
                        control={control}
                        name="basicInfoStep.databaseName"
                        placeholder="Database Name"
                        id="DbName"
                    />
                </Col>
            </Row>
            <Row className="mt-2">
                <IsEncryptedField />
            </Row>
        </div>
    );
}

function IsEncryptedField() {
    const { control, formState } = useFormContext<FormData>();

    const hasEncryption = useAppSelector(licenseSelectors.statusValue("HasEncryption"));
    const isSecureServer = useAppSelector(accessManagerSelectors.isSecureServer);

    return (
        <div className="d-flex align-items-center justify-content-center">
            <ConditionalPopover
                conditions={[
                    {
                        isActive: !hasEncryption,
                        message: <EncryptionUnavailableMessage />,
                    },
                    {
                        isActive: !isSecureServer,
                        message: <AuthenticationOffMessage />,
                    },
                ]}
                popoverPlacement="left"
            >
                <FormSwitch
                    color="primary"
                    control={control}
                    name="basicInfoStep.isEncrypted"
                    disabled={!hasEncryption || !isSecureServer || formState.isSubmitting}
                >
                    <Icon icon="encryption" />
                    Encrypt at Rest
                </FormSwitch>
            </ConditionalPopover>
            {hasEncryption && isSecureServer && (
                <>
                    <Icon icon="info" color="info" id="encryption-info" margin="ms-1" />
                    <UncontrolledPopover target="encryption-info" placement="right" trigger="hover">
                        <PopoverBody>
                            Data will be encrypted at the storage engine layer, using <code>XChaCha20-Poly1305</code>{" "}
                            authenticated encryption algorithm.
                        </PopoverBody>
                    </UncontrolledPopover>
                </>
            )}
        </div>
    );
}
