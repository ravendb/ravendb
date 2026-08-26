import { type FieldPath, type FieldValues, type UseControllerProps } from "react-hook-form";
import { FormSegmented } from "@/components/form/form-segmented";

export type FontSizeOption = {
    value: string;
    label: string;
    /** The size the widget renders at, or null for the option that hands the size to an input instead. */
    rem: number | null;
};

type ThemeEditorFontSizeFieldProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
> = UseControllerProps<TFieldValues, TName> & {
    label: string;
    disabled?: boolean;
    options: readonly FontSizeOption[];
    onValueChange?: (value: string) => void;
};

/**
 * The sizes as they read, not as they are named. The specimens are scaled up from the widget's own rem
 * values by a constant, so the steps keep their real proportions while staying far enough apart to see.
 */
const SPECIMEN_SCALE = 1;

export function ThemeEditorFontSizeField<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    options,
    ...props
}: ThemeEditorFontSizeFieldProps<TFieldValues, TName>) {
    return (
        <FormSegmented
            {...props}
            options={options.map((option) => ({
                value: option.value,
                label: option.label,
                preview:
                    option.rem === null ? undefined : (
                        <span
                            aria-hidden="true"
                            // leading-none keeps the glyph's own box centred in the cell: the inherited
                            // line height differs per size, which would tip the specimens off the row's
                            // centre line.
                            className="leading-none"
                            style={{ fontSize: `${option.rem * SPECIMEN_SCALE}rem` }}
                        >
                            Aa
                        </span>
                    ),
            }))}
        />
    );
}
