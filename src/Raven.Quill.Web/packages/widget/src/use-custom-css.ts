import { useEffect } from "react";

/** Preview-only: keeps a style element in the head with the operator's custom CSS, after the bundle's own
 *  stylesheets so it wins the cascade the same way it does live — where the server shell injects it
 *  instead, because there it can carry the CSP nonce the live page requires. */
export function useCustomCss(css: string | null): void {
    useEffect(() => {
        if (css === null || css.length === 0) return;

        const style = document.createElement("style");
        style.textContent = css;
        document.head.appendChild(style);
        return () => style.remove();
    }, [css]);
}
