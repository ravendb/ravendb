import { JSX, useRef } from "react";
import { useResizeObserver } from "hooks/useResizeObserver";

interface SizeGetterProps {
    render: (size: { width: number; height: number }) => JSX.Element;
    isHeighRequired?: boolean;
    className?: string;
}

export default function SizeGetter({ render, isHeighRequired = false, className }: SizeGetterProps) {
    const ref = useRef<HTMLDivElement>(null);

    const { width, height } = useResizeObserver({ ref });

    const canRender = !!(isHeighRequired ? width && height : width);

    return (
        <div ref={ref} style={{ height: "100%", width: "100%" }} className={className}>
            {canRender && render({ width, height })}
        </div>
    );
}
