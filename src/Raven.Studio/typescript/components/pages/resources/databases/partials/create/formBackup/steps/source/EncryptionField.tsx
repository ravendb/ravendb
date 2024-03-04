import { FormInput } from "components/common/Form";
import React from "react";
import { Row, Col, Label } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData, RestoreSource } from "../../createDatabaseFromBackupValidation";
import { useFormContext, useWatch } from "react-hook-form";

interface EncryptionFieldProps {
    sourceType: RestoreSource;
}

export default function EncryptionField({ sourceType }: EncryptionFieldProps) {
    const { control } = useFormContext<FormData>();

    const {
        sourceStep: {
            sourceData: { [sourceType]: selectedSourceData },
        },
    } = useWatch({
        control,
    });

    const isEncrypted = selectedSourceData.pointsWithTags[0].restorePoint?.isEncrypted;

    if (!isEncrypted) {
        return null;
    }

    return (
        <Row className="mt-2">
            <Col lg="3">
                <Label className="col-form-label">
                    Backup Encryption Key <small className="text-muted">(Base64 Encoding)</small>
                </Label>
            </Col>
            <Col>
                <FormInput
                    type="text"
                    control={control}
                    name={`sourceStep.sourceData.${sourceType}.encryptionKey`}
                    placeholder="Enter backup encryption key"
                />
            </Col>
        </Row>
    );
}
