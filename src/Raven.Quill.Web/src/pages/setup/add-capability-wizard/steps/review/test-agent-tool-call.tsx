import { useState } from "react";
import { Text } from "@/components/typography";
import { ChevronDown, ChevronUp, Database } from "lucide-react";
import type { AgentToolCall } from "@/api/custom-services/agent-stream";
import AceEditor from "@/components/ace-editor/ace-editor";
import type { AceEditorMode } from "@/components/ace-editor/ace-editor-types";
import { tryParseJson } from "@/lib/utils";

// A collapsed transcript of one query tool the agent ran this turn: the RQL it executed, the
// parameters the model filled in, and the content the query returned. Mirrors Studio's query
// tool transcript; the appliance supports query tools only, so there's no action/sub-agent variant.
export function TestQueryToolCall({ toolCall }: { toolCall: AgentToolCall }) {
    const [isExpanded, setIsExpanded] = useState(false);

    return (
        <div className="rounded-lg border bg-background text-sm">
            <button
                type="button"
                className="flex w-full items-center justify-between gap-2 p-2 text-left"
                aria-expanded={isExpanded}
                onClick={() => setIsExpanded((expanded) => !expanded)}
            >
                <span className="flex min-w-0 items-center gap-2 font-medium">
                    <Database className="size-4 shrink-0 text-muted-foreground" aria-hidden />
                    <span className="truncate">Query tool: {toolCall.name}</span>
                </span>
                {isExpanded ? (
                    <ChevronUp className="size-4 shrink-0 text-muted-foreground" aria-hidden />
                ) : (
                    <ChevronDown className="size-4 shrink-0 text-muted-foreground" aria-hidden />
                )}
            </button>

            {isExpanded && (
                <div className="grid gap-3 border-t p-2">
                    {toolCall.description && <Text variant="caption">{toolCall.description}</Text>}
                    {toolCall.query && <CodeField label="Query" value={toolCall.query} mode="sql" height="80px" />}
                    <CodeField
                        label="Parameters filled by the model"
                        value={prettifyJson(toolCall.arguments)}
                        mode="json"
                        height="60px"
                    />
                    {toolCall.result?.trim() && <ResultField result={toolCall.result} />}
                </div>
            )}
        </div>
    );
}

function ResultField({ result }: { result: string }) {
    // The query result is usually a JSON document set, but a tool can return plain text — render
    // JSON prettified, falling back to a plain-text editor when it isn't parseable.
    const parsed = tryParseJson(result);

    return parsed === undefined ? (
        <CodeField label="Result" value={result} mode="text" height="120px" />
    ) : (
        <CodeField label="Result" value={JSON.stringify(parsed, null, 4)} mode="json" height="120px" />
    );
}

function CodeField({
    label,
    value,
    mode,
    height,
}: {
    label: string;
    value: string;
    mode: AceEditorMode;
    height: string;
}) {
    return (
        <div className="grid gap-1">
            <Text variant="caption" as="span">
                {label}
            </Text>
            <div className="overflow-hidden rounded-md border">
                <AceEditor mode={mode} value={value} readOnly height={height} maxHeight={300} />
            </div>
        </div>
    );
}

// Pretty-prints a JSON string for display, leaving it untouched when it isn't valid JSON (e.g. an
// empty or partial arguments string).
function prettifyJson(value: string | null | undefined): string {
    if (!value) {
        return "";
    }

    const parsed = tryParseJson(value);
    return parsed === null ? value : JSON.stringify(parsed, null, 4);
}
