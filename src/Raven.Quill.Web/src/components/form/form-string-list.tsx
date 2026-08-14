// react-hook-form's formState is a mutable proxy; compiler memoization would freeze
// the array-level error message read below.
"use no memo";

import { Plus, Trash2 } from "lucide-react";
import {
    get,
    useFieldArray,
    useFormState,
    type ArrayPath,
    type FieldArray,
    type FieldError,
    type FieldPath,
    type FieldValues,
    type UseFieldArrayProps,
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
    disabled?: boolean;
    emptyLabel?: string;
    fieldName: (index: number) => FieldPath<TFieldValues>;
    itemLabel?: (index: number) => string;
    label: string;
    placeholder?: string;
};

export function FormStringList<TFieldValues extends FieldValues, TName extends ArrayPath<TFieldValues>>({
    addButtonLabel,
    control,
    defaultValue,
    description,
    disabled,
    emptyLabel = "No values.",
    fieldName,
    itemLabel,
    label,
    name,
    placeholder,
}: FormStringListProps<TFieldValues, TName>) {
    const fieldArray = useFieldArray({
        control,
        name,
    });

    const { errors } = useFormState({ control, name: name as unknown as FieldPath<TFieldValues> });
    const error = get(errors, name) as (FieldError & { root?: FieldError }) | undefined;
    const errorMessage = error?.message ?? error?.root?.message;

    return (
        <Field>
            <div className="flex items-center justify-between gap-3">
                <div>
                    <FieldLabel>{label}</FieldLabel>
                    {description && <FieldDescription>{description}</FieldDescription>}
                </div>
                <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={disabled}
                    onClick={() => fieldArray.append(defaultValue)}
                >
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
                            <FormInput
                                control={control}
                                name={fieldName(index)}
                                label={itemLabel?.(index)}
                                placeholder={placeholder}
                                disabled={disabled}
                            />
                            <Button
                                type="button"
                                variant="ghost"
                                size="icon"
                                className="self-end text-destructive"
                                disabled={disabled}
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
            {errorMessage && <FieldDescription className="text-destructive">{errorMessage}</FieldDescription>}
        </Field>
    );
}
