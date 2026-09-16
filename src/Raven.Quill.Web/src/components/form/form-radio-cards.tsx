import { type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { cn } from "@/lib/utils";

/**
 * Active state shared by every selectable card across the wizards: brand border,
 * a subtle brand gradient fill, and a soft ring. Cards with bespoke layouts that
 * can't use FormRadioCards still reuse this so the selected look stays consistent.
 */
export const SELECTED_CARD_CLASSES =
    "border-primary-strong bg-linear-to-br from-primary/15 to-transparent ring-2 ring-ring/25";

/**
 * Type scale for a card's title and its supporting line. Exported for the same bespoke cards, which
 * used to hardcode it and drifted from these every time the scale here changed.
 */
export const CARD_LABEL_CLASSES = "text-sm font-semibold";
export const CARD_DESCRIPTION_CLASSES = "mt-1 block text-sm leading-5 text-muted-foreground";

export type RadioCardOption<TValue extends string = string> = {
    value: TValue;
    label: ReactNode;
    description?: ReactNode;
    icon?: ReactNode;
    /** Rendered next to the label, e.g. a "Coming soon" badge. */
    badge?: ReactNode;
    disabled?: boolean;
    /**
     * Extra content rendered inside the card, below the header (e.g. an input).
     * Receives `select` so interactive content can select the card on focus.
     */
    content?: (helpers: { isSelected: boolean; select: () => void }) => ReactNode;
};

type FormRadioCardsProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
    TValue extends string,
> = UseControllerProps<TFieldValues, TName> & {
    options: ReadonlyArray<RadioCardOption<TValue>>;
    className?: string;
    disabled?: boolean;
};

export function FormRadioCards<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
    TValue extends string,
>({ className, control, defaultValue, disabled, name, options }: FormRadioCardsProps<TFieldValues, TName, TValue>) {
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({
        control,
        defaultValue,
        name,
    });

    return (
        <div className="grid gap-2">
            <div role="radiogroup" aria-invalid={invalid || undefined} className={cn("grid gap-3", className)}>
                {options.map((option) => {
                    const isSelected = option.value === value;
                    const isDisabled = disabled || formState.isSubmitting || option.disabled;
                    const select = () => onChange(option.value);

                    return (
                        <div
                            key={option.value}
                            className={cn(
                                "rounded-lg border bg-background transition-colors",
                                isSelected && SELECTED_CARD_CLASSES,
                                !isSelected && !isDisabled && "hover:bg-accent hover:text-accent-foreground",
                                isDisabled && "cursor-not-allowed opacity-55",
                            )}
                        >
                            <button
                                type="button"
                                role="radio"
                                aria-checked={isSelected}
                                disabled={isDisabled}
                                onClick={select}
                                // Keeps the padding on the header rather than the card so the whole
                                // block stays part of the click target. Its bottom edge is the gap to
                                // the content below, and stays under the card padding so the content
                                // reads as belonging to the card instead of floating in it.
                                className={cn(
                                    "block w-full text-left",
                                    option.content ? "rounded-t-lg px-4 pt-4 pb-3" : "rounded-lg p-4",
                                    isDisabled && "cursor-not-allowed",
                                )}
                            >
                                {option.icon && <span className="mb-2 block">{option.icon}</span>}
                                <span className={cn("flex items-center gap-2", CARD_LABEL_CLASSES)}>
                                    {option.label}
                                    {option.badge}
                                </span>
                                {option.description && (
                                    <span className={CARD_DESCRIPTION_CLASSES}>{option.description}</span>
                                )}
                            </button>
                            {option.content && (
                                <div className="px-4 pb-4">{option.content({ isSelected, select })}</div>
                            )}
                        </div>
                    );
                })}
            </div>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
        </div>
    );
}
