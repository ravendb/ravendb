import { type FieldPath, type FieldValues, type UseControllerProps } from "react-hook-form";
import { FormSegmented } from "@/components/form/form-segmented";

export type RadiusOption = {
    value: string;
    label: string;
    /** The corner the value actually draws, or "full" for a value that rounds the shape away entirely. */
    previewPx: number | "full";
};

type ThemeEditorRadiusFieldProps<
    TFieldValues extends FieldValues,
    TName extends FieldPath<TFieldValues>,
> = UseControllerProps<TFieldValues, TName> & {
    label: string;
    description?: React.ReactNode;
    disabled?: boolean;
    options: readonly RadiusOption[];
};

/** How much of the glyph the largest step in a scale is allowed to round. */
const GLYPH_MAX_RADIUS = 8;

/**
 * The rounding scale, each step drawn as the corner it produces. The steps are normalised against the
 * largest one in their own scale rather than drawn at their literal pixel value: the widget's corners
 * and a logo's corners are different scales, and at glyph size the raw numbers put the top steps within
 * a pixel or two of each other.
 */
export function ThemeEditorRadiusField<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    options,
    ...props
}: ThemeEditorRadiusFieldProps<TFieldValues, TName>) {
    const largestStep = Math.max(...options.map((option) => (option.previewPx === "full" ? 0 : option.previewPx)), 1);

    return (
        <FormSegmented
            {...props}
            options={options.map((option) => ({
                value: option.value,
                label: option.label,
                preview:
                    option.previewPx === "full" ? (
                        <PillPreview />
                    ) : (
                        <CornerPreview radius={(option.previewPx / largestStep) * GLYPH_MAX_RADIUS} />
                    ),
            }))}
        />
    );
}

/** One corner of a box, drawn at the option's radius: the smallest picture that still shows the change. */
function CornerPreview({ radius }: { radius: number }) {
    return (
        <svg viewBox="0 0 20 20" className="size-5" aria-hidden="true">
            <path
                d={`M 3 17 L 3 ${3 + radius} Q 3 3 ${3 + radius} 3 L 17 3`}
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
            />
        </svg>
    );
}

/** No corner left to draw: the value rounds the shape into a circle, so the glyph shows one. */
function PillPreview() {
    return (
        <svg viewBox="0 0 20 20" className="size-5" aria-hidden="true">
            <circle cx="10" cy="10" r="7" fill="none" stroke="currentColor" strokeWidth="2" />
        </svg>
    );
}
