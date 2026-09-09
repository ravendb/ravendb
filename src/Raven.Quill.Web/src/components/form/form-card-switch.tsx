import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldContent, FieldDescription, FieldLabel, FieldTitle } from "@/components/shadcn/ui/field";
import { Switch } from "@/components/shadcn/ui/switch";
import { cn } from "@/lib/utils";

type FormCardSwitchProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    disabled?: boolean;
    title: ReactNode;
    description: ReactNode;
};

export function FormCardSwitch<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    disabled,
    title,
    description,
    name,
}: FormCardSwitchProps<TFieldValues, TName>) {
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
        <FieldLabel htmlFor={generatedId} className={cn("cursor-pointer", className)} data-invalid={invalid}>
            <Field orientation="horizontal">
                <FieldContent>
                    <FieldTitle>{title}</FieldTitle>
                    <FieldDescription>{description}</FieldDescription>
                </FieldContent>
                <Switch
                    id={generatedId}
                    checked={!!value}
                    onCheckedChange={onChange}
                    disabled={disabled || formState.isSubmitting}
                    aria-invalid={invalid}
                />
            </Field>
        </FieldLabel>
    );
}
