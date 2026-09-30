import { useId, useState, type ChangeEvent, type ComponentProps, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Eye, EyeOff } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Input } from "@/components/shadcn/ui/input";
import { Field, FieldContent, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { InputGroup, InputGroupInput, InputGroupAddon } from "@/components/shadcn/ui/input-group";

type FormInputProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = ComponentProps<
    typeof Input
> &
    UseControllerProps<TFieldValues, TName> & {
        addons?: ReactNode;
        label?: ReactNode;
        description?: ReactNode;
        /**
         * "responsive" puts the label and description in a column beside the control, stacking again
         * below the field-group container's `md` breakpoint. Settings rows want it so the description
         * cannot stretch to the container's full width; a field in a dialog or wizard wants the
         * default. Requires a `FieldGroup` ancestor, which owns the container query.
         */
        orientation?: "vertical" | "responsive";
        afterChange?: (event: ChangeEvent<HTMLInputElement>) => void;
    };

export function FormInput<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    addons,
    className,
    placeholder,
    control,
    defaultValue,
    disabled,
    id,
    label,
    name,
    orientation = "vertical",
    type,
    description,
    afterChange,
    ...restProps
}: FormInputProps<TFieldValues, TName>) {
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
    const [isPasswordVisible, setIsPasswordVisible] = useState(false);

    const actualInputType = type === "password" && isPasswordVisible ? "text" : type;

    function handleValueChange(event: ChangeEvent<HTMLInputElement>) {
        const value = event.target.value;

        if (type === "number") {
            onChange(value === "" ? null : Number(value));
        } else {
            onChange(value);
        }

        afterChange?.(event);
    }

    const labelNode = label != null ? <FieldLabel htmlFor={inputId}>{label}</FieldLabel> : null;
    const errorNode = error?.message ? (
        <FieldDescription className="text-destructive">{error.message}</FieldDescription>
    ) : null;
    const descriptionNode = description ? <FieldDescription>{description}</FieldDescription> : null;

    const controlNode = (
        <InputGroup>
            <InputGroupInput
                id={inputId}
                placeholder={placeholder}
                onChange={handleValueChange}
                value={value ?? ""}
                ref={ref}
                onBlur={onBlur}
                type={actualInputType}
                disabled={disabled || formState.isSubmitting}
                aria-invalid={invalid}
                {...restProps}
            />
            {type === "password" && (
                <InputGroupAddon align="inline-end">
                    <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        onClick={() => setIsPasswordVisible((visible) => !visible)}
                    >
                        {isPasswordVisible ? <EyeOff /> : <Eye />}
                    </Button>
                </InputGroupAddon>
            )}
            {addons && addons}
        </InputGroup>
    );

    if (orientation === "responsive") {
        return (
            <Field orientation="responsive" className={className} data-invalid={invalid}>
                <FieldContent>
                    {labelNode}
                    {errorNode}
                    {descriptionNode}
                </FieldContent>
                {controlNode}
            </Field>
        );
    }

    return (
        <Field className={className} data-invalid={invalid}>
            {labelNode}
            {controlNode}
            {errorNode}
            {descriptionNode}
        </Field>
    );
}
