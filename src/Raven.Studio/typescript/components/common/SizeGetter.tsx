import { useRef } from "react";
import { useState, useEffect } from "react";

interface SizeGetterProps {
    render: (size: { width: number; height: number }) => JSX.Element;
    isHeighRequired?: boolean;
}

export default function SizeGetter({ render, isHeighRequired = false }: SizeGetterProps) {
    const ref = useRef<HTMLDivElement>();

    const [width, setWidth] = useState(0);
    const [height, setHeight] = useState(0);

    useEffect(() => {
        const currentRef = ref.current;
        const handleResize = () => {
            setWidth(currentRef.scrollWidth);
            setHeight(currentRef.scrollHeight);
        };

        currentRef.addEventListener("resize", handleResize);
        handleResize();

        return () => {
            currentRef.removeEventListener("resize", handleResize);
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [ref.current]);

    const canRender = !!(isHeighRequired ? width && height : width);

    return (
        <div ref={ref} style={{ height: "100%", width: "100%" }}>
            {canRender && render({ width, height })}
        </div>
    );
}
