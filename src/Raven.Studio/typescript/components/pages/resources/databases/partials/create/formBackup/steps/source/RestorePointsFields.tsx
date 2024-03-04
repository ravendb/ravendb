import React, { ReactNode } from "react";
import { FieldArrayWithId, useFormContext, useFieldArray, FieldPath, useWatch } from "react-hook-form";
import { Row, Col, Label, Button } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData, RestoreSource } from "../../createDatabaseFromBackupValidation";
import { Icon } from "components/common/Icon";

interface RestorePointsFieldsProps {
    pointsWithTagsFieldName: Extract<FieldPath<FormData>, `sourceStep.sourceData.${RestoreSource}.pointsWithTags`>;
    isSharded: boolean;
    mapRestorePoint: (field: FieldArrayWithId<FormData>, index: number) => ReactNode;
}

export default function RestorePointsFields({
    pointsWithTagsFieldName,
    isSharded,
    mapRestorePoint,
}: RestorePointsFieldsProps) {
    const { control, formState } = useFormContext<FormData>();

    const {
        sourceStep: { sourceType },
    } = useWatch({
        control,
    });

    const { fields, append } = useFieldArray({
        control,
        name: pointsWithTagsFieldName,
    });

    const pointsWithTagsErrorMessage = formState.errors.sourceStep?.sourceData?.[sourceType]?.pointsWithTags?.message;

    return (
        <>
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
            {pointsWithTagsErrorMessage && (
                <div className="position-absolute badge bg-danger rounded-pill margin-top-xxs">
                    {pointsWithTagsErrorMessage}
                </div>
            )}
        </>
    );
}
