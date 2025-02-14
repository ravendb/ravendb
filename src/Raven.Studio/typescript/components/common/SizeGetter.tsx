import { useRef } from "react";
import { useResizeObserver } from "hooks/useResizeObserver";

interface SizeGetterProps {
    render: (size: { width: number; height: number }) => JSX.Element;
    isHeighRequired?: boolean;
}

export default function SizeGetter({ render, isHeighRequired = false }: SizeGetterProps) {
    const ref = useRef<HTMLDivElement>();

    const { width, height } = useResizeObserver({ ref });

    const canRender = !!(isHeighRequired ? width && height : width);

    return (
        <div ref={ref} style={{ height: "100%", width: "100%" }}>
            {canRender && render({ width, height })}
        </div>
    );
}
