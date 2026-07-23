import { useEffect, useRef } from "react";
import { cn } from "@/lib/utils";

type WebWidgetStylePreviewProps = {
    previewHtml: string;
    css: string;
    className?: string;
};

const CUSTOM_STYLE_ID = "raven-custom";

function injectCss(doc: Document | null | undefined, css: string) {
    const styleEl = doc?.getElementById(CUSTOM_STYLE_ID);
    if (styleEl) {
        styleEl.textContent = css;
    }
}

export function WebWidgetStylePreview({ previewHtml, css, className }: WebWidgetStylePreviewProps) {
    const iframeRef = useRef<HTMLIFrameElement>(null);

    // Re-inject on every CSS edit (same document, no reload). A previewHtml change reloads
    // the iframe and is re-applied by onLoad once the new document is ready.
    useEffect(() => {
        injectCss(iframeRef.current?.contentDocument, css);
    }, [css]);

    return (
        <iframe
            ref={iframeRef}
            title="Web widget preview"
            // Defense in depth: blocks scripts if markup ever slips into previewHtml, while
            // allow-same-origin keeps contentDocument reachable for CSS injection.
            sandbox="allow-same-origin"
            srcDoc={previewHtml}
            onLoad={(event) => injectCss(event.currentTarget.contentDocument, css)}
            className={cn("h-full w-full border-0 bg-white", className)}
        />
    );
}
