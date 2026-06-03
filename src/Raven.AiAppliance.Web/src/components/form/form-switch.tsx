import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldLabel } from "@/components/shadcn/ui/field";
import { Switch } from "@/components/shadcn/ui/switch";

type FormSwitchProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    disabled?: boolean;
    label?: ReactNode;
};

export function FormSwitch<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
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
        <Field orientation="horizontal" className={className} data-invalid={invalid}>
            <Switch
                id={generatedId}
                checked={!!value}
                onCheckedChange={onChange}
                disabled={disabled || formState.isSubmitting}
                aria-invalid={invalid}
            />
            {label && <FieldLabel htmlFor={generatedId}>{label}</FieldLabel>}
        </Field>
    );
}
