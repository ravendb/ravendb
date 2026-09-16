import { useEffect } from "react";

/** Preview-only: live mode gets its font size from the server's root block. Set on `<html>`, so every
 *  rem-based size in the document scales with it. */
export function useDocumentFontSize(rem: number): void {
    useEffect(() => {
        document.documentElement.style.fontSize = `${rem}rem`;
        return () => {
            document.documentElement.style.fontSize = "";
        };
    }, [rem]);
}
