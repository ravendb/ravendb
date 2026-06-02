import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldContent, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Switch } from "@/components/shadcn/ui/switch";
import { cn } from "@/lib/utils";

type FormSwitchProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    label?: ReactNode;
};

export function FormSwitch<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    label,
    name,
}: FormSwitchProps<TFieldValues, TName>) {
    const generatedId = useId();
    const {
        field: { onChange, value },
        fieldState: { invalid },
        formState,
    } = useController({
        control,
        defaultValue,
        name,
    });

    return (
        <Field orientation="horizontal" className={cn("rounded-lg border p-3", className)} data-invalid={invalid}>
            <FieldContent>
                <FieldLabel htmlFor={generatedId}>{label}</FieldLabel>
                {description && <FieldDescription>{description}</FieldDescription>}
            </FieldContent>
            <Switch
                id={generatedId}
                checked={!!value}
                onCheckedChange={onChange}
                disabled={disabled || formState.isSubmitting}
                aria-invalid={invalid}
            />
        </Field>
    );
}
