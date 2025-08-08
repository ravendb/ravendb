import { JSX, useRef } from "react";
import { useResizeObserver } from "hooks/useResizeObserver";

interface SizeGetterProps {
    render: (size: { width: number; height: number }) => JSX.Element;
    isHeighRequired?: boolean;
    className?: string;
    style?: React.CSSProperties;
}

export default function SizeGetter({ render, isHeighRequired = false, className, style }: SizeGetterProps) {
    const ref = useRef<HTMLDivElement>(null);

    const { width, height } = useResizeObserver({ ref });

    const canRender = !!(isHeighRequired ? width && height : width);

    return (
        <div ref={ref} style={{ height: "100%", width: "100%", ...style }} className={className}>
            {canRender && render({ width, height })}
        </div>
    );
}
