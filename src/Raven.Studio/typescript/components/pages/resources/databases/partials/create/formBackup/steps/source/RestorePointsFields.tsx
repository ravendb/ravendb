import React, { ReactNode } from "react";
import { FieldArrayWithId, useFormContext, useFieldArray, FieldPath } from "react-hook-form";
import { Row, Col, Label, Button } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { Icon } from "components/common/Icon";

interface RestorePointsFieldsProps {
    restorePointsFieldName: Extract<FieldPath<FormData>, `sourceStep.sourceData.${restoreSource}.restorePoints`>;
    isSharded: boolean;
    mapRestorePoint: (field: FieldArrayWithId<FormData>, index: number) => ReactNode;
}

export default function RestorePointsFields({
    restorePointsFieldName,
    isSharded,
    mapRestorePoint,
}: RestorePointsFieldsProps) {
    const { control } = useFormContext<FormData>();

    const { fields, append } = useFieldArray({
        control,
        name: restorePointsFieldName,
    });

    return (
        <Row className="mt-2">
            <Col lg="3">
                <Label className="col-form-label">Restore Point</Label>
            </Col>
            <Col>
                {fields.map(mapRestorePoint)}
                {isSharded && (
                    <Button
                        size="sm"
                        color="shard"
                        className="rounded-pill mt-2"
                        onClick={() => append({ restorePoint: null, nodeTag: "" })}
                    >
                        <Icon icon="restore-backup" margin="m-0" /> Add shard restore point
                    </Button>
                )}
            </Col>
        </Row>
    );
}
