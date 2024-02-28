import { FormInput } from "components/common/Form";
import React from "react";
import { Row, Col, Label } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData, RestoreSource } from "../../createDatabaseFromBackupValidation";
import { FieldPath, useFormContext } from "react-hook-form";

interface EncryptionFieldProps {
    selectedSourceData: FormData["sourceStep"]["sourceData"][RestoreSource];
    encryptionKeyFieldName: Extract<FieldPath<FormData>, `sourceStep.sourceData.${RestoreSource}.encryptionKey`>;
}

export default function EncryptionField({ selectedSourceData, encryptionKeyFieldName }: EncryptionFieldProps) {
    const { control } = useFormContext<FormData>();

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
                    name={encryptionKeyFieldName}
                    placeholder="Enter backup encryption key"
                />
            </Col>
        </Row>
    );
}
