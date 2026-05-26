import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/shadcn/ui/select";
import { cn } from "@/lib/utils";

type FormSelectOption = {
    description?: string;
    disabled?: boolean;
    label: string;
    value: string;
};

type FormSelectProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    label?: ReactNode;
    options: readonly FormSelectOption[];
    placeholder?: string;
    triggerClassName?: string;
};

export function FormSelect<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    label,
    name,
    options,
    placeholder,
    triggerClassName,
}: FormSelectProps<TFieldValues, TName>) {
    const generatedId = useId();
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        control,
        defaultValue,
        name,
    });

    return (
        <Field className={className} data-invalid={invalid}>
            <FieldLabel htmlFor={generatedId}>{label}</FieldLabel>
            <Select
                value={typeof value === "string" ? value : ""}
                onValueChange={onChange}
                disabled={disabled || formState.isSubmitting}
            >
                <SelectTrigger id={generatedId} aria-invalid={invalid} className={cn("w-full", triggerClassName)}>
                    <SelectValue placeholder={placeholder} />
                </SelectTrigger>
                <SelectContent>
                    {options.map((option) => (
                        <SelectItem key={option.value} value={option.value} disabled={option.disabled}>
                            {option.label}
                        </SelectItem>
                    ))}
                </SelectContent>
            </Select>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
