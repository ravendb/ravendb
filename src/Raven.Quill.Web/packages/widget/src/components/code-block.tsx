import { useRef, useState } from "react";
import { CheckIcon, CopyIcon } from "@/components/icons";

const COPIED_RESET_MS = 2000;

type CodeBlockProps = {
    code: string;
    language: string | null;
};

export function CodeBlock({ code, language }: CodeBlockProps) {
    const [hasCopied, setHasCopied] = useState(false);
    const resetTimer = useRef<ReturnType<typeof setTimeout>>(undefined);

    const copy = async () => {
        try {
            await navigator.clipboard.writeText(code);
        } catch {
            // A denied clipboard permission is not worth surfacing in a chat transcript.
            return;
        }
        setHasCopied(true);
        clearTimeout(resetTimer.current);
        resetTimer.current = setTimeout(() => setHasCopied(false), COPIED_RESET_MS);
    };

    return (
        <div className="rounded-rq-sm border-rq-code-border bg-rq-code my-3 overflow-hidden border">
            <div className="border-rq-code-border flex items-center justify-between gap-2 border-b px-3 py-1.5">
                <span className="text-rq-muted font-mono text-[11px] tracking-wide uppercase">
                    {language ?? "code"}
                </span>
                <button
                    type="button"
                    onClick={copy}
                    aria-label={hasCopied ? "Code copied" : "Copy code"}
                    className="rounded-rq-sm text-rq-muted hover:text-rq-fg focus-visible:ring-rq-accent inline-flex items-center gap-1 px-1.5 py-1 text-xs transition-colors focus-visible:ring-2 focus-visible:outline-none"
                >
                    {hasCopied ? <CheckIcon className="size-3.5" /> : <CopyIcon className="size-3.5" />}
                    {hasCopied ? "Copied" : "Copy"}
                </button>
            </div>
            <pre className="overflow-x-auto px-3 py-2.5 text-[13px] leading-relaxed">
                <code className="font-mono">{code}</code>
            </pre>
        </div>
    );
}
