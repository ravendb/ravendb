import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import type { WidgetThemeColors } from "@/api/generated/server-api";
import { InfoHint } from "@/components/data/info-hint";
import { SELECTED_CARD_CLASSES } from "@/components/form/form-radio-cards";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";

type AppearanceValue = "Light" | "Dark" | "System";

type ThemeEditorAppearanceFieldProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
> = UseControllerProps<TFieldValues, TName> & {
    label: string;
    /** Carried by a help icon next to the label rather than a paragraph under the cards. */
    hint?: string;
    disabled?: boolean;
    /** The colors the widget is currently being themed with, so each card previews the visitor's real view. */
    light: WidgetThemeColors;
    dark: WidgetThemeColors;
    /** Runs after the form value updates, for view state that follows this field (e.g. the live preview). */
    onValueChange?: (value: AppearanceValue) => void;
};

/**
 * Appearance picker built as preview cards rather than a toggle group: the choice is about how the
 * widget looks, so each option shows a miniature of it in the visitor's own colors. "System" is the
 * same miniature split down the diagonal, standing for "whichever the visitor already prefers".
 */
export function ThemeEditorAppearanceField<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    dark,
    defaultValue,
    disabled,
    hint,
    label,
    light,
    name,
    onValueChange,
}: ThemeEditorAppearanceFieldProps<TFieldValues, TName>) {
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({ control, defaultValue, name });

    const isDisabled = disabled || formState.isSubmitting;

    const options: ReadonlyArray<{ value: AppearanceValue; caption: string; preview: React.ReactNode }> = [
        { value: "Light", caption: "Always light", preview: <SchemeMock colors={light} scheme="Light" /> },
        { value: "Dark", caption: "Always dark", preview: <SchemeMock colors={dark} scheme="Dark" /> },
        {
            value: "System",
            caption: "Follows visitor",
            preview: (
                <>
                    <SchemeMock colors={light} scheme="Light" />
                    {/* One miniature, split by a slanted edge: the left of it stays light, the right
                        turns dark. The slant crosses the answer lines and leaves the visitor's bubble on
                        the dark side, so both schemes show a recognisable piece of the widget. */}
                    <div
                        className="absolute inset-0"
                        style={{ clipPath: "polygon(62% 0, 100% 0, 100% 100%, 38% 100%)" }}
                    >
                        <SchemeMock colors={dark} scheme="Dark" />
                    </div>
                </>
            ),
        },
    ];

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
                className="grid grid-cols-3 gap-2"
            >
                {options.map((option) => {
                    const isSelected = option.value === value;

                    return (
                        <button
                            key={option.value}
                            type="button"
                            role="radio"
                            aria-checked={isSelected}
                            disabled={isDisabled}
                            onClick={() => {
                                onChange(option.value);
                                onValueChange?.(option.value);
                            }}
                            className={cn(
                                // flex-col, not block: the grid stretches every card to the tallest one,
                                // and a <button> centres its content in the space it's given - which left
                                // the shorter cards' labels sitting lower than the others'.
                                "flex flex-col overflow-hidden rounded-lg border bg-background text-left transition-colors focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none",
                                isSelected && SELECTED_CARD_CLASSES,
                                !isSelected && !isDisabled && "hover:border-primary-strong/50",
                                isDisabled && "cursor-not-allowed opacity-55",
                            )}
                        >
                            <span aria-hidden="true" className="relative block h-16 overflow-hidden border-b">
                                {option.preview}
                            </span>
                            <span className="block px-2.5 py-2">
                                <Text as="span" variant="label" className="flex items-center justify-between gap-1.5">
                                    {option.value}
                                    <span
                                        aria-hidden="true"
                                        className={cn(
                                            "size-3 shrink-0 rounded-full border-2 border-muted-foreground/50",
                                            isSelected &&
                                                "border-primary-strong bg-primary-strong ring-2 ring-primary-strong/40",
                                        )}
                                    />
                                </Text>
                                {/* What the card means in words, so the choice doesn't rest on reading the
                                    miniature - and so the section below it needs no prose to explain "System". */}
                                <Text as="span" variant="caption" className="mt-0.5 block">
                                    {option.caption}
                                </Text>
                            </span>
                        </button>
                    );
                })}
            </div>
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
        </div>
    );
}

/** A postage-stamp widget: the visitor's message, then the answer that follows it. */
function SchemeMock({ colors, scheme }: { colors: WidgetThemeColors; scheme: "Light" | "Dark" }) {
    return (
        <span
            className="absolute inset-0 block p-2"
            style={{
                backgroundColor: colors.backgroundColor,
                // The answer lines below are drawn from currentColor, so this is what keeps them readable
                // on whatever background the channel picked for the scheme.
                color: scheme === "Dark" ? "#ffffff" : "#000000",
            }}
        >
            <span className="ml-auto block h-3 w-8 rounded-sm" style={{ backgroundColor: colors.messageColor }} />
            <span className="mt-2 block h-1 w-full rounded-full bg-current opacity-15" />
            <span className="mt-1 block h-1 w-full rounded-full bg-current opacity-15" />
            <span className="mt-1 block h-1 w-2/3 rounded-full bg-current opacity-15" />
        </span>
    );
}
