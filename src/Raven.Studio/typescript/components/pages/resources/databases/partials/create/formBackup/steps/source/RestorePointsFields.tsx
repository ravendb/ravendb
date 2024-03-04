import React, { ReactNode } from "react";
import { FieldArrayWithId, useFormContext, useFieldArray, useWatch } from "react-hook-form";
import { Row, Col, Label, Button } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { Icon } from "components/common/Icon";

interface RestorePointsFieldsProps {
    mapRestorePoint: (field: FieldArrayWithId<FormData>, index: number) => ReactNode;
}

export default function RestorePointsFields({ mapRestorePoint }: RestorePointsFieldsProps) {
    const { control, formState } = useFormContext<FormData>();

    const {
        sourceStep: { sourceType },
        basicInfoStep: { isSharded },
    } = useWatch({
        control,
    });

    const { fields, append } = useFieldArray({
        control,
        name: `sourceStep.sourceData.${sourceType}.pointsWithTags`,
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
