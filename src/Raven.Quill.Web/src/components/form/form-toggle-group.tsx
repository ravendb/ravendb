import { type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";

export type FormToggleGroupOption = {
    value: string;
    label: ReactNode;
    ariaLabel?: string;
};

type FormToggleGroupProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    label?: ReactNode;
    options: readonly FormToggleGroupOption[];
};

/** Single-select toggle group; clicking the selected item again clears the value back to null. */
export function FormToggleGroup<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    label,
    name,
    options,
}: FormToggleGroupProps<TFieldValues, TName>) {
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
            {label && <FieldLabel>{label}</FieldLabel>}
            <ToggleGroup
                type="single"
                variant="outline"
                value={typeof value === "string" ? value : ""}
                onValueChange={(next) => onChange(next === "" ? null : next)}
                disabled={disabled || formState.isSubmitting}
                aria-invalid={invalid}
            >
                {options.map((option) => (
                    <ToggleGroupItem key={option.value} value={option.value} aria-label={option.ariaLabel}>
                        {option.label}
                    </ToggleGroupItem>
                ))}
            </ToggleGroup>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
