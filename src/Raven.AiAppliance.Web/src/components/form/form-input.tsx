import { useId, useState, type ComponentProps, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Eye, EyeOff } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Input } from "@/components/shadcn/ui/input";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { InputGroup, InputGroupInput, InputGroupAddon } from "@/components/shadcn/ui/input-group";

type FormInputProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = ComponentProps<
    typeof Input
> &
    UseControllerProps<TFieldValues, TName> & {
        addons?: ReactNode;
        label?: ReactNode;
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
    type,
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

    function handleValueChange(value: string) {
        if (type === "number") {
            onChange(value === "" ? null : Number(value));
            return;
        }

        onChange(value);
    }

    return (
        <Field className={className} data-invalid={invalid}>
            <FieldLabel htmlFor={inputId}>{label}</FieldLabel>
            <InputGroup>
                <InputGroupInput
                    id={inputId}
                    placeholder={placeholder}
                    onChange={(e) => handleValueChange(e.target.value)}
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
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
        </Field>
    );
}
