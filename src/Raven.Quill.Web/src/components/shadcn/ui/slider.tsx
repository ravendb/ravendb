import * as React from "react";
import { Slider as SliderPrimitive } from "radix-ui";

import { cn } from "@/lib/utils";

// The variants below are spelled `data-[orientation=…]` rather than shadcn's generated
// `data-horizontal:` / `data-vertical:`: those look for bare `data-horizontal` / `data-vertical`
// attributes, and this Radix version writes `data-orientation` instead - so none of them matched and
// the track rendered 0px tall.
function Slider({
    className,
    defaultValue,
    value,
    min = 0,
    max = 100,
    thumbProps: { className: thumbClassName, ...thumbProps } = {},
    rangeClassName,
    rootOverlay,
    trackClassName,
    trackOverlay,
    ...props
}: React.ComponentProps<typeof SliderPrimitive.Root> & {
    /** Radix puts role="slider" on the thumb, so the accessible name, aria-valuetext and anything else
     *  belonging to the control itself has to reach the thumb rather than the root. */
    thumbProps?: React.ComponentProps<typeof SliderPrimitive.Thumb>;
    /** For decorating the rail itself, e.g. drawing the stops of a stepped scale. */
    trackClassName?: string;
    /** Drawn inside the rail, above the filled range but below the thumb - the layer a stepped scale's
     *  stops belong on, so they stay visible on both sides of the thumb. */
    trackOverlay?: React.ReactNode;
    /** For hiding or restyling the filled range, e.g. when an overlay draws its own. */
    rangeClassName?: string;
    /** Drawn last, above the thumb - for anything that has to stay readable when the thumb passes
     *  under it, such as a value printed inside the control. */
    rootOverlay?: React.ReactNode;
}) {
    const _values = React.useMemo(
        () => (Array.isArray(value) ? value : Array.isArray(defaultValue) ? defaultValue : [min, max]),
        [value, defaultValue, min, max],
    );

    return (
        <SliderPrimitive.Root
            data-slot="slider"
            defaultValue={defaultValue}
            value={value}
            min={min}
            max={max}
            className={cn(
                "relative flex w-full touch-none items-center select-none data-disabled:opacity-50 data-[orientation=vertical]:h-full data-[orientation=vertical]:min-h-40 data-[orientation=vertical]:w-auto data-[orientation=vertical]:flex-col",
                className,
            )}
            {...props}
        >
            <SliderPrimitive.Track
                data-slot="slider-track"
                className={cn(
                    "relative grow overflow-hidden rounded-full bg-muted data-[orientation=horizontal]:h-1.5 data-[orientation=horizontal]:w-full data-[orientation=vertical]:h-full data-[orientation=vertical]:w-1.5",
                    trackClassName,
                )}
            >
                <SliderPrimitive.Range
                    data-slot="slider-range"
                    className={cn(
                        "absolute bg-primary select-none data-[orientation=horizontal]:h-full data-[orientation=vertical]:w-full",
                        rangeClassName,
                    )}
                />
                {trackOverlay}
            </SliderPrimitive.Track>
            {Array.from({ length: _values.length }, (_, index) => (
                <SliderPrimitive.Thumb
                    data-slot="slider-thumb"
                    key={index}
                    {...thumbProps}
                    className={cn(
                        "relative block size-4 shrink-0 rounded-full border border-ring bg-white ring-ring/50 transition-[color,box-shadow] select-none after:absolute after:-inset-2 hover:ring-3 focus-visible:ring-3 focus-visible:outline-hidden active:ring-3 disabled:pointer-events-none disabled:opacity-50",
                        thumbClassName,
                    )}
                />
            ))}
            {rootOverlay}
        </SliderPrimitive.Root>
    );
}

export { Slider };
