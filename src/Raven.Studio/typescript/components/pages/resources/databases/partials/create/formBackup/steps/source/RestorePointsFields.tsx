import React from "react";
import { useFormContext, useFieldArray, useWatch } from "react-hook-form";
import { Row, Col, Label, Button } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { Icon } from "components/common/Icon";

export interface RestorePointElementProps {
    index: number;
    remove: () => void;
}

interface RestorePointsFieldsProps {
    restorePointElement: React.ComponentType<RestorePointElementProps>;
}

export default function RestorePointsFields(props: RestorePointsFieldsProps) {
    const { control, formState } = useFormContext<FormData>();

    const {
        sourceStep: { sourceType },
        basicInfoStep: { isSharded },
    } = useWatch({
        control,
    });

    const { fields, append, remove } = useFieldArray({
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
                <Col lg={isSharded ? 12 : 9}>
                    {fields.map((field, idx) => (
                        <props.restorePointElement key={field.id} index={idx} remove={() => remove(idx)} />
                    ))}
                    {isSharded && (
                        <Button
                            size="sm"
                            color="shard"
                            className="rounded-pill"
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
