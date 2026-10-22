/* eslint-disable react-hooks/incompatible-library */
// React Compiler memoization is disabled here: useVirtualizer returns a mutable instance whose
// scroll-driven state must be re-read on every render.
"use no memo";

import { useRef, type ReactNode } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { cn } from "@/lib/utils";

const OVERSCAN_ROWS = 8;

/** Rows measure themselves, so they may wrap. `children` follow the rows inside the same scroll. */
export function PlannerVirtualList<Item>({
    items,
    getKey,
    estimateSize,
    renderItem,
    className,
    children,
}: {
    items: Item[];
    getKey: (item: Item) => string;
    estimateSize: (item: Item) => number;
    renderItem: (item: Item) => ReactNode;
    className?: string;
    children?: ReactNode;
}) {
    const scrollRef = useRef<HTMLDivElement>(null);

    const virtualizer = useVirtualizer({
        count: items.length,
        getScrollElement: () => scrollRef.current,
        estimateSize: (index) => estimateSize(items[index]),
        getItemKey: (index) => getKey(items[index]),
        overscan: OVERSCAN_ROWS,
    });

    return (
        <div ref={scrollRef} className={cn("min-h-0 flex-1 overflow-y-auto", className)}>
            <div className="relative" style={{ height: virtualizer.getTotalSize() }}>
                {virtualizer.getVirtualItems().map((row) => (
                    <div
                        key={row.key}
                        ref={virtualizer.measureElement}
                        data-index={row.index}
                        className="absolute inset-x-0"
                        style={{ top: row.start }}
                    >
                        {renderItem(items[row.index])}
                    </div>
                ))}
            </div>
            {children}
        </div>
    );
}
