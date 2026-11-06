import { Text } from "@/components/typography";
import { type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { InfoHint } from "@/components/data/info-hint";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";

export type SegmentedOption<TValue extends string = string> = {
    value: TValue;
    label: string;
    /** What the cell shows. Options with nothing to draw fall back to their label. */
    preview?: ReactNode;
};

type FormSegmentedProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    label: string;
    description?: ReactNode;
    /** Sits behind a help icon next to the label, for guidance too long to keep on screen. */
    hint?: string;
    disabled?: boolean;
    options: readonly SegmentedOption[];
    /** Runs after the form value updates, for state that follows this field. */
    onValueChange?: (value: string) => void;
};

/**
 * A short, ordered scale shown as one control rather than a list: each cell previews the value it
 * stands for, so the choice is made by looking rather than by reading names. The label still names
 * every cell for assistive tech and on hover, since the previews carry no text of their own.
 */
export function FormSegmented<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    defaultValue,
    description,
    disabled,
    hint,
    label,
    name,
    onValueChange,
    options,
}: FormSegmentedProps<TFieldValues, TName>) {
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({ control, defaultValue, name });

    const isDisabled = disabled || formState.isSubmitting;

    return (
        <div className="grid gap-2">
            <span className="flex items-center gap-1.5">
                <Text as="span" id={`${name}-label`} variant="label">
                    {label}
                </Text>
                {hint && <InfoHint content={hint} />}
            </span>
            <div
                role="radiogroup"
                aria-labelledby={`${name}-label`}
                aria-invalid={invalid || undefined}
                className="flex overflow-hidden rounded-lg border bg-background"
            >
                {options.map((option) => {
                    const isSelected = option.value === value;

                    return (
                        <button
                            key={option.value}
                            type="button"
                            role="radio"
                            aria-checked={isSelected}
                            aria-label={option.label}
                            title={option.label}
                            disabled={isDisabled}
                            onClick={() => {
                                onChange(option.value);
                                onValueChange?.(option.value);
                            }}
                            className={cn(
                                "flex h-9 flex-1 items-center justify-center border-l transition-colors first:border-l-0 focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none focus-visible:ring-inset",
                                isSelected && "bg-muted font-medium text-foreground",
                                !isSelected && !isDisabled && "text-muted-foreground hover:bg-accent",
                                isDisabled && "cursor-not-allowed opacity-55",
                            )}
                        >
                            {option.preview ?? <span className="text-sm">{option.label}</span>}
                        </button>
                    );
                })}
            </div>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </div>
    );
}
