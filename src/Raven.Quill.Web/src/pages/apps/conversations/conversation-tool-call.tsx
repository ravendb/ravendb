import { Wrench } from "lucide-react";
import type { AiToolCallResult } from "@/api/generated/server-api";
import { Parameters } from "@/components/data/parameters";
import { CodeBlock, TranscriptDisclosure } from "@/pages/apps/conversations/transcript-disclosure";
import { Text } from "@/components/typography";
import { tryParseJson } from "@/lib/utils";

// A collapsed transcript entry for one tool the agent ran during a turn. Kept operator-friendly:
// only the parameters the model filled in and the tool's response — no raw query internals.
export function ConversationToolCall({
    disclosureKey,
    toolCall,
}: {
    disclosureKey: string;
    toolCall: AiToolCallResult;
}) {
    return (
        <TranscriptDisclosure disclosureKey={disclosureKey} icon={Wrench} label={toolCall.name || "Tool call"}>
            <div className="grid gap-3 border-t px-3 py-3">
                <ToolCallParameters rawArguments={toolCall.arguments} />
                <ToolCallResponse result={toolCall.result} />
            </div>
        </TranscriptDisclosure>
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
            <Text as="span" variant="caption" className="font-medium">
                {label}
            </Text>
            {children}
        </div>
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
    return parsed === null ? value : JSON.stringify(parsed, null, 2);
}
