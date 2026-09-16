import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/shadcn/ui/input-group";

/** <input type="color"> only accepts #rrggbb, so a shorthand or half-typed value falls back to black. */
function toSwatchValue(value: string): string {
    const color = (value ?? "").trim().toLowerCase();
    if (/^#[0-9a-f]{6}$/.test(color)) return color;
    if (/^#[0-9a-f]{3}$/.test(color)) return `#${[...color.slice(1)].map((digit) => digit + digit).join("")}`;
    return "#000000";
}

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

/** A hex color field: a native swatch, a free-text hex input, and optional preset swatches. */
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
                    <input
                        type="color"
                        aria-label={typeof label === "string" ? `${label} picker` : "Color picker"}
                        className="size-5 shrink-0 cursor-pointer appearance-none rounded-full border-none bg-transparent p-0 ring-1 ring-foreground/20 ring-inset disabled:cursor-not-allowed"
                        value={toSwatchValue(value)}
                        disabled={disabled}
                        onChange={(event) => onChange(event.target.value)}
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

            {presets && presets.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                    {presets.map((preset) => (
                        <button
                            key={preset}
                            type="button"
                            disabled={disabled}
                            aria-label={`Use ${preset}`}
                            style={{ background: preset }}
                            onClick={() => onChange(preset)}
                            className="size-5 rounded-full ring-1 ring-foreground/20 transition-transform ring-inset hover:scale-110 focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed"
                        />
                    ))}
                </div>
            )}

            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
