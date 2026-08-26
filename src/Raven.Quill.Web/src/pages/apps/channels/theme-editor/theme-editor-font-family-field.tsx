import { Text } from "@/components/typography";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { SELECTED_CARD_CLASSES } from "@/components/form/form-radio-cards";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";

export type FontFamilyOption = {
    /** The CSS font stack this option saves, and the one its specimen is rendered in. */
    stack: string;
    label: string;
};

type ThemeEditorFontFamilyFieldProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
> = UseControllerProps<TFieldValues, TName> & {
    label: string;
    disabled?: boolean;
    options: readonly FontFamilyOption[];
};

/**
 * Fonts pick themselves once you can see them: each option is a specimen set in its own stack, so the
 * two serifs are told apart by their shapes rather than by the words "Serif" and "Transitional serif".
 */
export function ThemeEditorFontFamilyField<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    defaultValue,
    disabled,
    label,
    name,
    options,
}: ThemeEditorFontFamilyFieldProps<TFieldValues, TName>) {
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({ control, defaultValue, name });

    const isDisabled = disabled || formState.isSubmitting;

    return (
        <div className="grid gap-2">
            <Text as="span" id={`${name}-label`} variant="label">
                {label}
            </Text>
            <div
                role="radiogroup"
                aria-labelledby={`${name}-label`}
                aria-invalid={invalid || undefined}
                className="grid grid-cols-3 gap-2"
            >
                {options.map((option) => {
                    const isSelected = option.stack === value;

                    return (
                        <button
                            key={option.stack}
                            type="button"
                            role="radio"
                            aria-checked={isSelected}
                            disabled={isDisabled}
                            onClick={() => onChange(option.stack)}
                            // Three to a row leaves no space for the longer names, so the tooltip carries
                            // what the label has to truncate.
                            title={option.label}
                            className={cn(
                                "rounded-lg border bg-background px-3 py-2 text-left transition-colors focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none",
                                isSelected && SELECTED_CARD_CLASSES,
                                !isSelected && !isDisabled && "hover:border-primary-strong/50",
                                isDisabled && "cursor-not-allowed opacity-55",
                            )}
                        >
                            <span
                                aria-hidden="true"
                                className="block text-xl leading-6"
                                style={{ fontFamily: option.stack }}
                            >
                                Aa
                            </span>
                            <Text as="span" variant="caption" className="mt-0.5 block truncate">
                                {option.label}
                            </Text>
                        </button>
                    );
                })}
            </div>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
        </div>
    );
}
