import { useId, type ComponentProps, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Textarea } from "@/components/shadcn/ui/textarea";

type FormTextareaProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = ComponentProps<
    typeof Textarea
> &
    UseControllerProps<TFieldValues, TName> & {
        description?: ReactNode;
        label?: ReactNode;
        labelClassName?: string;
        textareaClassName?: string;
    };

export function FormTextarea<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    id,
    label,
    labelClassName,
    name,
    textareaClassName,
    ...restProps
}: FormTextareaProps<TFieldValues, TName>) {
    const generatedId = useId();
    const inputId = id ?? generatedId;
    const {
        field: { onBlur, onChange, ref, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        control,
        defaultValue,
        name,
    });

    return (
        <Field className={className} data-invalid={invalid}>
            {/* Field lays its children out with a gap, so an empty label would leave a stray one.
                Callers that label the field from outside pass aria-labelledby instead. */}
            {label && (
                <FieldLabel htmlFor={inputId} className={labelClassName}>
                    {label}
                </FieldLabel>
            )}
            <Textarea
                id={inputId}
                onBlur={onBlur}
                onChange={onChange}
                ref={ref}
                value={value ?? ""}
                disabled={disabled || formState.isSubmitting}
                aria-invalid={invalid}
                className={textareaClassName}
                {...restProps}
            />
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
