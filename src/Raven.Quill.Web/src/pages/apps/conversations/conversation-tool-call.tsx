import { useState } from "react";
import { ChevronDown, ChevronUp, Wrench } from "lucide-react";
import type { AiToolCallResult } from "@/api/generated/server-api";

// A collapsed transcript entry for one tool the agent ran during a turn. Kept operator-friendly:
// only the parameters the model filled in and the tool's response — no raw query internals.
export function ConversationToolCall({ toolCall }: { toolCall: AiToolCallResult }) {
    const [isExpanded, setIsExpanded] = useState(false);

    return (
        <div className="w-full rounded-lg border bg-background text-sm">
            <button
                type="button"
                className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left"
                aria-expanded={isExpanded}
                onClick={() => setIsExpanded((expanded) => !expanded)}
            >
                <span className="flex min-w-0 items-center gap-2">
                    <Wrench className="size-4 shrink-0 text-muted-foreground" aria-hidden />
                    <span className="truncate font-medium">{toolCall.name || "Tool call"}</span>
                </span>
                {isExpanded ? (
                    <ChevronUp className="size-4 shrink-0 text-muted-foreground" aria-hidden />
                ) : (
                    <ChevronDown className="size-4 shrink-0 text-muted-foreground" aria-hidden />
                )}
            </button>

            {isExpanded && (
                <div className="grid gap-3 border-t px-3 py-2">
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

    return (
        <ToolCallSection label="Parameters">
            {parameters.length === 0 ? (
                <p className="text-xs text-muted-foreground">No parameters</p>
            ) : (
                <dl className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs">
                    {parameters.map(([name, value]) => (
                        <div key={name} className="col-span-2 grid grid-cols-subgrid">
                            <dt className="font-mono text-muted-foreground">{name}</dt>
                            <dd className="min-w-0 font-medium break-words">{formatParameterValue(value)}</dd>
                        </div>
                    ))}
                </dl>
            )}
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
        <div className="grid gap-1">
            <span className="text-xs font-medium text-muted-foreground">{label}</span>
            {children}
        </div>
    );
}

function CodeBlock({ value }: { value: string }) {
    return (
        <pre className="max-h-60 overflow-auto rounded-md border bg-muted/40 p-2 font-mono text-xs whitespace-pre-wrap">
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
