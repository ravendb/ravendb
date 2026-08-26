import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { ColorPickerPopover } from "@/components/form/color-picker-popover";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/shadcn/ui/input-group";

type FormColorPickerProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    label?: ReactNode;
    /** Swatches the operator can pick with one click, e.g. a brand palette. */
    presets?: readonly string[];
};

/** A hex color field: a swatch that opens the color picker popover, and a free-text hex input. */
export function FormColorPicker<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    label,
    name,
    presets,
}: FormColorPickerProps<TFieldValues, TName>) {
    const id = useId();
    const {
        field: { onChange, onBlur, value },
        fieldState: { error, invalid },
    } = useController({ control, defaultValue, name });

    return (
        <Field className={className} data-invalid={invalid || undefined}>
            {label && <FieldLabel htmlFor={id}>{label}</FieldLabel>}
            <InputGroup>
                <InputGroupAddon>
                    <ColorPickerPopover
                        value={value ?? ""}
                        onChange={onChange}
                        presets={presets}
                        disabled={disabled}
                        label={typeof label === "string" ? label : undefined}
                    />
                </InputGroupAddon>
                <InputGroupInput
                    id={id}
                    type="text"
                    spellCheck={false}
                    autoComplete="off"
                    value={value ?? ""}
                    disabled={disabled}
                    onBlur={onBlur}
                    aria-invalid={invalid}
                    onChange={(event) => onChange(event.target.value)}
                />
            </InputGroup>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}

            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
