import { type ReactNode } from "react";
import { type FieldPath, type FieldValues, type UseControllerProps, useController } from "react-hook-form";
import { FieldDescription } from "@/components/shadcn/ui/field";
import { Slider } from "@/components/shadcn/ui/slider";

type FormSliderProps<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>> = UseControllerProps<
    TFieldValues,
    TName
> & {
    label: string;
    description?: ReactNode;
    disabled?: boolean;
    min: number;
    max: number;
    step: number;
    /** Shown inside the row, and read out to assistive tech in place of the raw number. */
    format: (value: number) => string;
    /** Where the handle sits while the field holds no number of its own yet. */
    fallback: number;
};

/**
 * A scrubber rather than a rail with a knob on it: the row itself is the control, carrying its own name
 * on the left and its value on the right, with the filled part reading as a proportion behind them. It
 * costs one row of the panel instead of three (label, rail, value), and dragging anywhere in the row -
 * or clicking a spot - moves it.
 */
export function FormSlider<TFieldValues extends FieldValues, TName extends FieldPath<TFieldValues>>({
    control,
    defaultValue,
    description,
    disabled,
    fallback,
    format,
    label,
    max,
    min,
    name,
    step,
}: FormSliderProps<TFieldValues, TName>) {
    const {
        field: { onChange, value },
        fieldState: { error, invalid },
        formState,
    } = useController({ control, defaultValue, name });

    const current = typeof value === "number" ? value : fallback;
    const stops = Math.round((max - min) / step);
    const fraction = (current - min) / (max - min);

    return (
        <div className="grid gap-2">
            <Slider
                // The row is the control, so it carries the surface, the rounding and the clipping that
                // keeps the fill inside it. The fill runs the row's whole width; the padding below is
                // what the handle keeps from the row's edges, inside that fill.
                className="group h-10 w-full cursor-pointer overflow-hidden rounded-lg border bg-background"
                style={{ "--pad": "0.375rem", "--thumb": "5px", "--fill": fraction } as React.CSSProperties}
                min={min}
                max={max}
                step={step}
                value={[current]}
                onValueChange={([next]) => onChange(next)}
                disabled={disabled || formState.isSubmitting}
                aria-invalid={invalid}
                trackClassName="h-full w-full rounded-lg bg-transparent data-[orientation=horizontal]:h-full"
                // The overlay draws the fill instead: the range's own width runs to the row's edge and
                // paints under the stops, and this fill has to end just past the handle and over them.
                rangeClassName="hidden"
                trackOverlay={
                    <>
                        {/* Hairline stops, interior only, on the span the handle actually travels. */}
                        <span
                            aria-hidden="true"
                            className="pointer-events-none absolute inset-y-0"
                            style={{ left: "var(--pad)", right: "var(--pad)" }}
                        >
                            {Array.from({ length: stops - 1 }, (_, index) => (
                                <span
                                    key={index}
                                    className="absolute top-1/2 h-2 w-px -translate-x-1/2 -translate-y-1/2 rounded-full bg-border"
                                    style={{ left: `${((index + 1) / stops) * 100}%` }}
                                />
                            ))}
                        </span>
                        {/* A solid surface rather than a translucent wash, so the filled part reads the
                            same over whatever the panel puts behind it, and it sits over the stops
                            rather than under them, so only the ones still to come show. It runs --pad
                            past the handle, so the handle sits inside a rounded end rather than on its
                            edge - and at the maximum that trailing pad lands on the row's own edge. */}
                        <span
                            aria-hidden="true"
                            className="pointer-events-none absolute inset-y-0 left-0 rounded-lg bg-muted"
                            style={{ width: "calc(2 * var(--pad) + var(--fill) * (100% - 2 * var(--pad)))" }}
                        />
                    </>
                }
                // Above the handle, not under it: the handle travels past the value, and a number that
                // disappears when it does is worse than a handle that reads as passing behind it.
                rootOverlay={
                    <>
                        <span className="pointer-events-none absolute inset-y-0 left-3.5 flex items-center text-[0.8125rem] font-medium text-muted-foreground">
                            {label}
                        </span>
                        <span className="pointer-events-none absolute inset-y-0 right-3 flex items-center font-mono text-[0.8125rem] font-medium tabular-nums">
                            {format(current)}
                        </span>
                    </>
                }
                thumbProps={{
                    "aria-label": label,
                    "aria-valuetext": format(current),
                    // A bar, not a knob, and solid rather than washed out: it marks where the value sits,
                    // so it has to stay legible over the fill. Small at rest, growing into the row on
                    // hover, focus and while dragging.
                    className:
                        "h-[26px] w-[5px] scale-[0.7] rounded-full border-0 bg-foreground opacity-[0.33] shadow-none transition-[scale,opacity] hover:ring-0 focus-visible:scale-100 focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-ring/50 group-hover:scale-100 group-hover:opacity-100 group-active:scale-100 group-active:opacity-100 active:ring-0",
                    // Radix walks the handle from half its own width to half from the far end. Nudging
                    // it by the difference between that and --pad keeps it --pad from both edges, so it
                    // stays inside the fill rather than running into the row's corners. Set as
                    // `translate` rather than a transform, which the scale above already owns.
                    style: { translate: "calc((0.5 - var(--fill)) * (2 * var(--pad) - var(--thumb))) 0" },
                }}
            />
            {error?.message && <FieldDescription className="text-destructive">{error.message}</FieldDescription>}
            {description && <FieldDescription>{description}</FieldDescription>}
        </div>
    );
}
