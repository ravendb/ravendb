import { useState } from "react";
import { ChevronDown, ChevronUp, Wrench } from "lucide-react";
import type { AiToolCallResult } from "@/api/generated/server-api";
import { Parameters } from "@/components/data/parameters";

// A collapsed transcript entry for one tool the agent ran during a turn. Kept operator-friendly:
// only the parameters the model filled in and the tool's response — no raw query internals.
export function ConversationToolCall({ toolCall }: { toolCall: AiToolCallResult }) {
    const [isExpanded, setIsExpanded] = useState(false);

    return (
        <div className="w-full overflow-hidden rounded-lg border bg-muted/40 text-sm">
            <button
                type="button"
                className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left transition-colors hover:bg-muted/70"
                aria-expanded={isExpanded}
                onClick={() => setIsExpanded((expanded) => !expanded)}
            >
                <span className="flex min-w-0 items-center gap-2">
                    <span className="flex size-6 shrink-0 items-center justify-center rounded-md bg-primary/10 text-primary-strong">
                        <Wrench className="size-3.5" aria-hidden />
                    </span>
                    <span className="truncate font-medium">{toolCall.name || "Tool call"}</span>
                </span>
                {isExpanded ? (
                    <ChevronUp className="size-4 shrink-0 text-muted-foreground" aria-hidden />
                ) : (
                    <ChevronDown className="size-4 shrink-0 text-muted-foreground" aria-hidden />
                )}
            </button>

            {isExpanded && (
                <div className="grid gap-3 border-t px-3 py-3">
                    <ToolCallParameters rawArguments={toolCall.arguments} />
                    <ToolCallResponse result={toolCall.result} />
                </div>
            )}
        </div>
    );
}

function ToolCallParameters({ rawArguments }: { rawArguments: string | null | undefined }) {
    const parameters = parseParameterEntries(rawArguments);

    if (!parameters) {
        // Arguments that aren't a JSON object (partial or plain text) are still worth showing raw.
        return rawArguments?.trim() ? (
            <ToolCallSection label="Parameters">
                <CodeBlock value={rawArguments} />
            </ToolCallSection>
        ) : null;
    }

    if (parameters.length === 0) {
        return null;
    }

    return (
        <ToolCallSection label="Parameters">
            <Parameters
                className="flex-wrap"
                params={parameters.map(([name, value]) => ({ name, value: formatParameterValue(value) }))}
            />
        </ToolCallSection>
    );
}

function ToolCallResponse({ result }: { result: string | null | undefined }) {
    if (!result?.trim()) {
        return null;
    }

    return (
        <ToolCallSection label="Response">
            <CodeBlock value={prettifyJson(result)} />
        </ToolCallSection>
    );
}

function ToolCallSection({ label, children }: { label: string; children: React.ReactNode }) {
    return (
        <div className="grid gap-1.5">
            <span className="text-xs font-medium text-muted-foreground">{label}</span>
            {children}
        </div>
    );
}

function CodeBlock({ value }: { value: string }) {
    return (
        <pre className="max-h-60 overflow-auto rounded-md border bg-background p-2 font-mono text-xs whitespace-pre-wrap">
            {value}
        </pre>
    );
}

// The model fills tool arguments as a JSON object string; surface it as name/value rows. Null when
// the string isn't a JSON object so the caller can fall back to the raw text.
function parseParameterEntries(rawArguments: string | null | undefined): [string, unknown][] | null {
    if (!rawArguments?.trim()) {
        return [];
    }

    const parsed = tryParseJson(rawArguments);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        return null;
    }

    return Object.entries(parsed);
}

function formatParameterValue(value: unknown): string {
    return typeof value === "string" ? value : JSON.stringify(value);
}

function prettifyJson(value: string): string {
    const parsed = tryParseJson(value);
    return parsed === undefined ? value : JSON.stringify(parsed, null, 2);
}

function tryParseJson(value: string): unknown {
    try {
        return JSON.parse(value);
    } catch {
        return undefined;
    }
}
