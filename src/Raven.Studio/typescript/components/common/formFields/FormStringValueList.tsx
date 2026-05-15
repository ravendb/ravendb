import { EmptySet } from "components/common/EmptySet";
import { FormErrorIcon, FormGroup, FormInput } from "components/common/Form";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { ArrayPath, FieldArray, FieldPath, FieldValues, useFieldArray, UseFieldArrayProps } from "react-hook-form";

type FormStringValueListProps<
    TFieldValues extends FieldValues = FieldValues,
    TName extends ArrayPath<TFieldValues> = ArrayPath<TFieldValues>,
> = UseFieldArrayProps<TFieldValues, TName> & {
    title: React.ReactNode;
    addButtonLabel: React.ReactNode;
    defaultValue: FieldArray<TFieldValues, TName>;
    fieldNameAccessor: (idx: number) => FieldPath<TFieldValues>;
    className?: string;
};

export default function FormStringValueList<
    TFieldValues extends FieldValues = FieldValues,
    TName extends ArrayPath<TFieldValues> = ArrayPath<TFieldValues>,
>({
    control,
    name,
    title,
    addButtonLabel,
    defaultValue,
    fieldNameAccessor,
    className,
}: FormStringValueListProps<TFieldValues, TName>) {
    const fieldArray = useFieldArray({
        control,
        name,
    });

    return (
        <div className={className}>
            <div className="hstack justify-content-between mb-1">
                <div className="hstack">
                    {title}
                    <FormErrorIcon control={control} paths={[name]} />
                </div>
                <Button variant="link" size="sm" onClick={() => fieldArray.append(defaultValue)}>
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
                        <FormGroup key={field.id} marginClass="m-0">
                            <FormInput
                                type="text"
                                control={control}
                                name={fieldNameAccessor(idx)}
                                addon={
                                    <Button
                                        variant="link"
                                        className="text-danger"
                                        size="sm"
                                        onClick={() => fieldArray.remove(idx)}
                                    >
                                        <Icon icon="trash" margin="m-0" />
                                    </Button>
                                }
                            />
                        </FormGroup>
                    ))}
                </div>
            )}
        </div>
    );
}
