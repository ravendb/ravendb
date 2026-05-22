import { Plus, Trash2 } from "lucide-react";
import {
    type ArrayPath,
    type FieldArray,
    type FieldPath,
    type FieldValues,
    type UseFieldArrayProps,
    useFieldArray,
} from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { Button } from "@/components/shadcn/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";

type FormStringListProps<TFieldValues extends FieldValues, TName extends ArrayPath<TFieldValues>> = UseFieldArrayProps<
    TFieldValues,
    TName
> & {
    addButtonLabel: string;
    defaultValue: FieldArray<TFieldValues, TName>;
    description?: string;
    emptyLabel?: string;
    fieldName: (index: number) => FieldPath<TFieldValues>;
    itemLabel?: (index: number) => string | undefined;
    label: string;
};

export function FormStringList<TFieldValues extends FieldValues, TName extends ArrayPath<TFieldValues>>({
    addButtonLabel,
    control,
    defaultValue,
    description,
    emptyLabel = "No values.",
    fieldName,
    itemLabel,
    label,
    name,
}: FormStringListProps<TFieldValues, TName>) {
    const fieldArray = useFieldArray({
        control,
        name,
    });

    return (
        <Field>
            <div className="flex items-center justify-between gap-3">
                <div>
                    <FieldLabel>{label}</FieldLabel>
                    {description && <FieldDescription>{description}</FieldDescription>}
                </div>
                <Button type="button" variant="outline" size="sm" onClick={() => fieldArray.append(defaultValue)}>
                    <Plus className="size-4" aria-hidden />
                    {addButtonLabel}
                </Button>
            </div>

            {fieldArray.fields.length === 0 ? (
                <div className="rounded-md border bg-background px-3 py-4 text-center text-sm text-muted-foreground">
                    {emptyLabel}
                </div>
            ) : (
                <div className="grid gap-2">
                    {fieldArray.fields.map((field, index) => (
                        <div key={field.id} className="grid gap-2 md:grid-cols-[1fr_auto]">
                            <FormInput control={control} name={fieldName(index)} label={itemLabel?.(index)} />
                            <Button
                                type="button"
                                variant="ghost"
                                size="icon"
                                className="self-end text-destructive"
                                onClick={() => fieldArray.remove(index)}
                                aria-label="Remove value"
                                title="Remove value"
                            >
                                <Trash2 className="size-4" aria-hidden />
                            </Button>
                        </div>
                    ))}
                </div>
            )}
        </Field>
    );
}
