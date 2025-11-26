import { useState, useEffect } from "react";

export default function useClassNamesObserver(element: Element) {
    const [classNames, setClassNames] = useState(element?.classList?.value ?? "");

    useEffect(() => {
        if (!element) {
            return;
        }

        const observer = new MutationObserver((mutations) => {
            for (const mutation of mutations) {
                if (mutation.attributeName === "class") {
                    setClassNames(element.classList.value);
                }
            }
        });

        observer.observe(element, { attributes: true });

        return () => observer.disconnect();
    }, []);

    const hasClassNames = (classNamesToCheck: string[]) => {
        return classNamesToCheck.every((className) => classNames.includes(className));
    };

    return { classNames, hasClassNames };
}
