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
    "border-primary bg-linear-to-br from-primary/15 to-transparent ring-2 ring-ring/25";

export type RadioCardOption<TValue extends string = string> = {
    value: TValue;
    label: ReactNode;
    description?: ReactNode;
    icon?: ReactNode;
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
                                className={cn(
                                    "block w-full p-4 text-left",
                                    option.content ? "rounded-t-lg" : "rounded-lg",
                                    isDisabled && "cursor-not-allowed",
                                )}
                            >
                                {option.icon && <span className="mb-3 block">{option.icon}</span>}
                                <span className="block text-sm font-semibold">{option.label}</span>
                                {option.description && (
                                    <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                                        {option.description}
                                    </span>
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
