import { useEffect, useRef } from "react";

export const useThemeColors = () => {
    const activeColor = useRef("#ffffff");
    const baseColor = useRef("#333333");

    useEffect(() => {
        const updateColors = () => {
            const bodyStyle = getComputedStyle(document.body);
            const rootStyle = getComputedStyle(document.documentElement);

            activeColor.current =
                bodyStyle.getPropertyValue("--text-muted").trim() ||
                rootStyle.getPropertyValue("--text-muted").trim() ||
                activeColor.current;

            baseColor.current =
                bodyStyle.getPropertyValue("--panel-bg-2").trim() ||
                rootStyle.getPropertyValue("--panel-bg-2").trim() ||
                baseColor.current;
        };

        const observer = new MutationObserver(() => setTimeout(updateColors, 20));
        observer.observe(document.body, { attributes: true, attributeFilter: ["class", "data-theme"] });
        observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class", "data-theme"] });

        updateColors();

        return () => observer.disconnect();
    }, []);

    return { activeColor, baseColor };
};
