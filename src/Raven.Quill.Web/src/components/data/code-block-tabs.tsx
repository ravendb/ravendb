import { Copy } from "lucide-react";
import { highlightCode, type HighlightLanguage } from "@/components/ace-editor/static-highlight";
import { Button } from "@/components/shadcn/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { cn, copyToClipboard } from "@/lib/utils";
import { Text } from "@/components/typography";

export type CodeTab = {
    value: string;
    label: string;
    code: string;
    language: HighlightLanguage;
};

type CodeBlockTabsProps = {
    tabs: CodeTab[];
    value: string;
    onValueChange?: (value: string) => void;
    copyLabel: string;
    // Cap the code viewport at this many lines; taller snippets scroll. Omit to size the viewport to the
    // shortest snippet (so a multi-language block never resizes when the language changes).
    maxLines?: number;
    // Let the code viewport grow with its content instead of being fixed, but never below this many
    // lines. Takes precedence over `maxLines`.
    minLines?: number;
    className?: string;
};

// A code block whose language selector lives in the card's header toolbar (tabs on the left, copy on
// the right) rather than as a detached row above it — the standard dev-docs pattern. With a single tab
// the selector is dropped and only the copy button remains, so it doubles as a plain code block.
export function CodeBlockTabs({
    tabs,
    value,
    onValueChange,
    copyLabel,
    maxLines,
    minLines,
    className,
}: CodeBlockTabsProps) {
    const activeTab = tabs.find((tab) => tab.value === value);
    const activeCode = activeTab?.code ?? "";
    const hasSelector = tabs.length > 1;

    // Fix the code viewport height so switching languages never resizes the card: size it to the shortest
    // snippet, capped by `maxLines`. `1lh` is the pre's own line height and `1rem` is the py-2 padding
    // (border-box), so the box holds exactly that many lines; anything taller scrolls. With `minLines`
    // the height is a floor instead, so the block fits its content and only reserves that many lines.
    const shortestSnippetLines = Math.min(...tabs.map((tab) => tab.code.split("\n").length));
    const bodyLines = maxLines ? Math.min(shortestSnippetLines, maxLines) : shortestSnippetLines;
    const bodyStyle = minLines
        ? { minHeight: `calc(${minLines} * 1lh + 1rem)` }
        : { height: `calc(${bodyLines} * 1lh + 1rem)` };

    return (
        <Tabs
            value={value}
            onValueChange={onValueChange}
            className={cn("min-w-0 gap-0 overflow-hidden rounded-lg border bg-muted/50", className)}
        >
            <div className="flex h-9 items-center justify-between gap-2 border-b bg-muted/50 pr-1.5 pl-1">
                {hasSelector ? (
                    <TabsList variant="line" className="h-9">
                        {tabs.map((tab) => (
                            <TabsTrigger key={tab.value} value={tab.value} className="text-xs">
                                {tab.label}
                            </TabsTrigger>
                        ))}
                    </TabsList>
                ) : (
                    // A single snippet has no selector; its label stands in as the code block's title so the
                    // header still reads as a labelled bar rather than a lone copy button.
                    <Text variant="caption" as="span" className="pl-2 font-medium">
                        {activeTab?.label}
                    </Text>
                )}
                <Button
                    type="button"
                    variant="ghost"
                    size="icon-sm"
                    className="ml-auto"
                    aria-label={copyLabel}
                    onClick={() => copyToClipboard(activeCode)}
                >
                    <Copy className="size-3.5" aria-hidden="true" />
                </Button>
            </div>
            {tabs.map((tab) => (
                <TabsContent key={tab.value} value={tab.value}>
                    <pre
                        className="overflow-y-auto px-3 py-2 text-xs break-all whitespace-pre-wrap [font-variant-ligatures:none]"
                        style={bodyStyle}
                    >
                        <code dangerouslySetInnerHTML={{ __html: highlightCode(tab.code, tab.language) }} />
                    </pre>
                </TabsContent>
            ))}
        </Tabs>
    );
}
