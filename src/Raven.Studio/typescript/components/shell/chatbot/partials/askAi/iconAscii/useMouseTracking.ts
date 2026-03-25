import { useRef, useEffect } from "react";

export const useMouseTracking = () => {
    const mouseRef = useRef({ x: -1000, y: -1000 });

    useEffect(() => {
        const handleMouseMove = (event: MouseEvent) => {
            mouseRef.current = { x: event.clientX, y: event.clientY };
        };

        const handleMouseLeave = () => {
            mouseRef.current = { x: -1000, y: -1000 };
        };

        window.addEventListener("mousemove", handleMouseMove);
        window.addEventListener("mouseleave", handleMouseLeave);

        return () => {
            window.removeEventListener("mousemove", handleMouseMove);
            window.removeEventListener("mouseleave", handleMouseLeave);
        };
    }, []);

    return mouseRef;
};
