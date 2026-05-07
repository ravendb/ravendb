import { EmptySet } from "components/common/EmptySet";
import { FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import Button from "react-bootstrap/Button";
import { FieldPath, useFieldArray, useFormContext } from "react-hook-form";

type PrimaryKeyColumnsPath<T = FieldPath<EditCdcSinkTaskFormData>> = T extends `${string}primaryKeyColumns` ? T : never;
type JoinColumnsPath<T = FieldPath<EditCdcSinkTaskFormData>> = T extends `${string}joinColumns` ? T : never;

interface EditCdcSinkTaskStringValueListProps {
    title: string;
    addButtonLabel: string;
    path: PrimaryKeyColumnsPath | JoinColumnsPath;
}

export default function EditCdcSinkTaskStringValueList({
    title,
    addButtonLabel,
    path,
}: EditCdcSinkTaskStringValueListProps) {
    const { control } = useFormContext<EditCdcSinkTaskFormData>();

    const fieldArray = useFieldArray({
        control,
        name: path,
    });

    return (
        <div className="mb-2">
            <div className="hstack justify-content-between mb-1">
                <div>{title}</div>
                <Button variant="link" size="sm" onClick={() => fieldArray.append({ value: "" })}>
                    <Icon icon="plus" />
                    {addButtonLabel}
                </Button>
            </div>
            {fieldArray.fields.length === 0 ? (
                <div className="panel-bg-2 p-1 rounded border border-secondary hstack justify-content-center">
                    <EmptySet compact>No values defined.</EmptySet>
                </div>
            ) : (
                <div className="vstack gap-1">
                    {fieldArray.fields.map((field, idx) => (
                        <div key={field.id} className="hstack gap-1">
                            <FormInput type="text" control={control} name={`${path}.${idx}.value`} />
                            <Button
                                variant="link"
                                className="text-danger"
                                size="sm"
                                onClick={() => fieldArray.remove(idx)}
                            >
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}
