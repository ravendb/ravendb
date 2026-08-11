import { Copy } from "lucide-react";
import { highlightCode, type HighlightLanguage } from "@/components/ace-editor/static-highlight";
import { Button } from "@/components/shadcn/ui/button";
import { cn, copyToClipboard } from "@/lib/utils";

type CopyableCodeProps = {
    code: string;
    copyLabel: string;
    language?: HighlightLanguage;
    className?: string;
};

export function CopyableCode({ code, copyLabel, language, className }: CopyableCodeProps) {
    return (
        <div className="relative min-w-0">
            <pre
                className={cn(
                    "rounded-lg border bg-muted/50 py-2 pr-10 pl-3 text-xs break-all whitespace-pre-wrap [font-variant-ligatures:none]",
                    className,
                )}
            >
                {language ? (
                    <code dangerouslySetInnerHTML={{ __html: highlightCode(code, language) }} />
                ) : (
                    <code>{code}</code>
                )}
            </pre>
            <Button
                type="button"
                variant="ghost"
                size="icon-sm"
                className="absolute top-1.5 right-1.5"
                aria-label={copyLabel}
                onClick={() => copyToClipboard(code)}
            >
                <Copy className="size-3.5" aria-hidden="true" />
            </Button>
        </div>
    );
}
