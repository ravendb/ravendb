import React, { useLayoutEffect, useRef, useState } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import "./SegmentedControl.scss";

export interface SegmentedControlItem<T extends string> {
    value: T;
    label: string;
    icon?: IconName;
    count?: number;
}

interface SegmentedControlProps<T extends string> {
    items: SegmentedControlItem<T>[];
    selected: T;
    onSelect: (value: T) => void;
    // stretch edge-to-edge with equal-width segments (e.g. a row of tabs) instead of sizing to content
    fullWidth?: boolean;
    className?: string;
}

interface IndicatorRect {
    left: number;
    top: number;
    width: number;
    height: number;
}

// A pill-style segmented control - one button per item. A single highlight pill sits behind the
// selected button and drifts to whichever button is clicked. Each item may carry an icon and/or a
// count badge.
export default function SegmentedControl<T extends string>({
    items,
    selected,
    onSelect,
    fullWidth,
    className,
}: SegmentedControlProps<T>) {
    const containerRef = useRef<HTMLDivElement>(null);
    const buttonRefs = useRef<Record<string, HTMLButtonElement>>({});
    const [indicator, setIndicator] = useState<IndicatorRect | null>(null);

    // Position the highlight pill over the selected button. Measured in a layout effect so the first
    // paint already shows it in place (no slide-in from the corner); later selection changes animate
    // via the CSS transition. A ResizeObserver re-measures when the container width shifts the segments.
    useLayoutEffect(() => {
        const container = containerRef.current;
        if (!container) {
            return;
        }
        const measure = () => {
            const btn = buttonRefs.current[selected];
            if (!btn) {
                return;
            }
            const next: IndicatorRect = {
                left: btn.offsetLeft,
                top: btn.offsetTop,
                width: btn.offsetWidth,
                height: btn.offsetHeight,
            };
            // keep the same reference when nothing moved so a ResizeObserver tick doesn't re-render
            setIndicator((prev) =>
                prev &&
                prev.left === next.left &&
                prev.top === next.top &&
                prev.width === next.width &&
                prev.height === next.height
                    ? prev
                    : next
            );
        };
        measure();
        const observer = new ResizeObserver(measure);
        observer.observe(container);
        return () => observer.disconnect();
    }, [selected]);

    return (
        <div
            ref={containerRef}
            className={classNames("segmented-control", { "segmented-control--full-width": fullWidth }, className)}
            role="tablist"
        >
            {indicator && (
                <span
                    className="segmented-control-indicator"
                    style={{
                        width: indicator.width,
                        height: indicator.height,
                        transform: `translate(${indicator.left}px, ${indicator.top}px)`,
                    }}
                />
            )}
            {items.map((item) => (
                <button
                    key={item.value}
                    ref={(el) => {
                        if (el) {
                            buttonRefs.current[item.value] = el;
                        }
                    }}
                    type="button"
                    role="tab"
                    aria-selected={item.value === selected}
                    className={classNames("segmented-control-btn", { active: item.value === selected })}
                    onClick={() => onSelect(item.value)}
                >
                    {item.icon && <Icon icon={item.icon} margin="m-0" />}
                    <span>{item.label}</span>
                    {item.count != null && (
                        <span className="segmented-control-count">{item.count.toLocaleString()}</span>
                    )}
                </button>
            ))}
        </div>
    );
}
