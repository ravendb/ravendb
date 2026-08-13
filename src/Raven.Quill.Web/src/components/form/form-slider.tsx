import { useId, type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { Field, FieldDescription, FieldLabel } from "@/components/shadcn/ui/field";
import { Slider } from "@/components/shadcn/ui/slider";

type FormSliderProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    className?: string;
    description?: ReactNode;
    disabled?: boolean;
    label?: ReactNode;
    min: number;
    max: number;
    step?: number;
    /** Renders the current value next to the label, e.g. `(value) => `${value}px``. */
    formatValue?: (value: number) => string;
};

export function FormSlider<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    className,
    control,
    defaultValue,
    description,
    disabled,
    formatValue,
    label,
    max,
    min,
    name,
    step = 1,
}: FormSliderProps<TFieldValues, TName>) {
    const id = useId();
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
    } = useController({ control, defaultValue, name });

    const current = typeof value === "number" ? value : min;

    return (
        <Field className={className} data-invalid={invalid || undefined}>
            {label && (
                <FieldLabel htmlFor={id} className="justify-between">
                    <span>{label}</span>
                    {formatValue && <span className="font-normal text-muted-foreground">{formatValue(current)}</span>}
                </FieldLabel>
            )}
            <Slider
                id={id}
                min={min}
                max={max}
                step={step}
                disabled={disabled}
                value={[current]}
                onValueChange={([next]) => onChange(next)}
            />
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </Field>
    );
}
