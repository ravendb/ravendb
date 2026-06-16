import { useState } from "react";
import { type Control, useFieldArray, useForm, useFormContext, useWatch } from "react-hook-form";
import { useParams } from "react-router";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { Bot, ChevronDown, ChevronUp, FlaskConical, MessageSquare, Send, Settings2, Trash2 } from "lucide-react";
import { api } from "@/api/api";
import type { AgentToolCall } from "@/api/custom-services/agent-stream";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { buildAgentConfigurationPayload } from "@/pages/setup/add-capability-wizard/agent-config-form";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import {
    Sheet,
    SheetContent,
    SheetDescription,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/shadcn/ui/sheet";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormTextarea } from "@/components/form/form-textarea";
import AceEditor from "@/components/ace-editor/ace-editor";
import { TestQueryToolCall } from "@/pages/setup/add-capability-wizard/steps/review/test-agent-tool-call";
import { withNestedSubmit } from "@/lib/form-utils";

// Footer action for the wizard's Review step: opens a sheet to chat with the draft agent.
// The button stays disabled until the draft has the minimum a test needs (name, system
// prompt, and an AI provider connection).
export function ReviewTestAgentButton() {
    const { control } = useFormContext<AgentFormData>();
    const [name, systemPrompt, connectionStringName] = useWatch({
        control,
        name: ["review.name", "review.systemPrompt", "connection.connectionStringName"],
    });
    const [isOpen, setIsOpen] = useState(false);

    const isReady = Boolean(name?.trim() && systemPrompt?.trim() && connectionStringName);

    return (
        <Sheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>
                <Button
                    type="button"
                    variant="outline"
                    disabled={!isReady}
                    title={
                        isReady ? undefined : "Add a name, system prompt, and AI provider connection before testing."
                    }
                >
                    <FlaskConical className="size-4" aria-hidden />
                    Test agent
                </Button>
            </SheetTrigger>
            <SheetContent className="flex w-full flex-col gap-0 sm:max-w-lg data-[side=right]:sm:max-w-lg">
                <SheetHeader className="border-b">
                    <SheetTitle>Test agent</SheetTitle>
                    <SheetDescription>
                        Chat with the draft agent to check its answers. Each message runs a fresh, unsaved turn.
                    </SheetDescription>
                </SheetHeader>
                <TestAgentPanel />
            </SheetContent>
        </Sheet>
    );
}

// Agent answers always render as JSON: `json` holds the live answer (the sample response
// shape with the streamed field filling in) and is swapped for the full structured answer
// once the turn finishes. `toolCalls` are the query tools the agent ran (filled on `done`).
// `text` carries plain user prompts and error messages.
type ChatMessage = {
    id: string;
    role: "user" | "agent" | "error";
    text: string;
    json?: string;
    toolCalls?: AgentToolCall[];
};

const testFormSchema = z.object({
    prompt: z.string(),
    // Which output field streams token-by-token; empty lets the server pick the first field.
    streamField: z.string(),
    parameters: z.array(z.object({ name: z.string(), value: z.string().trim().min(1, "Value is required") })),
});

type TestFormData = z.infer<typeof testFormSchema>;

function TestAgentPanel() {
    const { slug = "" } = useParams();
    const wizardForm = useFormContext<AgentFormData>();
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [isStreaming, setIsStreaming] = useState(false);
    const [areParametersCollapsed, setAreParametersCollapsed] = useState(false);

    // The draft's declared output fields (sample object first, schema as fallback) — the
    // options for the "Streamed field" select. Read once from the wizard form on open.
    const [streamFieldOptions] = useState(() =>
        getOutputFieldNames(wizardForm.getValues("review.sampleObject"), wizardForm.getValues("review.outputSchema")),
    );

    const form = useForm<TestFormData>({
        resolver: zodResolver(testFormSchema),
        defaultValues: {
            prompt: "",
            // Default to the first declared field — the server's own convention when unset.
            streamField: streamFieldOptions[0] ?? "",
            // The agent's declared parameters, ready for the operator to fill in values.
            parameters: wizardForm.getValues("review.parameters").map((parameter) => ({
                name: parameter.name,
                value: "",
            })),
        },
    });
    const parameterFields = useFieldArray({ control: form.control, name: "parameters" });
    const prompt = useWatch({ control: form.control, name: "prompt" });
    const streamFieldSelectOptions: FormSelectOption<string>[] = streamFieldOptions.map((name) => ({
        value: name,
        label: name,
    }));

    async function send(values: TestFormData) {
        const trimmedPrompt = values.prompt.trim();
        if (!trimmedPrompt || isStreaming) {
            return;
        }

        // Re-read the wizard form so the test always runs the latest draft.
        const wizardValues = wizardForm.getValues();
        const configuration = buildAgentConfigurationPayload(wizardValues);
        const parameters = toParameterRecord(values.parameters);
        // The streamed field fills into this declared answer shape, so the live answer reads
        // as a real JSON object while it streams in. The shape comes from the sample response
        // object, or the Response JSON schema when only that is provided.
        const answerShape = getOutputShape(wizardValues.review.sampleObject, wizardValues.review.outputSchema);
        const { streamField } = values;

        const agentMessageId = crypto.randomUUID();
        setMessages((previous) => [
            ...previous,
            { id: crypto.randomUUID(), role: "user", text: trimmedPrompt },
            { id: agentMessageId, role: "agent", text: "", json: buildStreamingJson(answerShape, streamField, "") },
        ]);
        form.setValue("prompt", "");
        setIsStreaming(true);

        let streamedText = "";
        try {
            for await (const event of api.services.agentTest.stream(slug, {
                prompt: trimmedPrompt,
                configuration,
                parameters,
                streamField: streamField || null,
            })) {
                if (event.type === "chunk") {
                    streamedText += event.text;
                    const json = buildStreamingJson(answerShape, streamField, streamedText);
                    setMessages((previous) =>
                        replaceMessage(previous, agentMessageId, (message) => ({
                            ...message,
                            text: streamedText,
                            json,
                        })),
                    );
                } else if (event.type === "done") {
                    // Swap the live answer for the full structured output, keeping the streamed
                    // JSON if the server returned no structured answer. Attach the query tools the
                    // agent ran so the transcript can show them above the answer.
                    const json =
                        toAnswerJson(event.fullAnswer ?? event.answer) ??
                        buildStreamingJson(answerShape, streamField, streamedText);
                    const toolCalls = event.toolCalls ?? [];
                    setMessages((previous) =>
                        replaceMessage(previous, agentMessageId, (message) => ({ ...message, json, toolCalls })),
                    );
                } else if (event.type === "error") {
                    setMessages((previous) =>
                        replaceMessage(previous, agentMessageId, () => ({
                            id: agentMessageId,
                            role: "error",
                            text: event.message,
                        })),
                    );
                }
            }
        } catch (error) {
            const message = error instanceof Error ? error.message : "Agent test failed.";
            setMessages((previous) =>
                replaceMessage(previous, agentMessageId, () => ({ id: agentMessageId, role: "error", text: message })),
            );
        } finally {
            setIsStreaming(false);
        }
    }

    function clearChat() {
        setMessages([]);
        form.setValue("prompt", "");
    }

    // Reveal the parameters when a send is blocked by a missing value, so the validation
    // errors are never hidden behind a collapsed section.
    const handleSend = form.handleSubmit(send, () => setAreParametersCollapsed(false));

    return (
        <div className="flex min-h-0 flex-1 flex-col">
            <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
                {streamFieldSelectOptions.length > 1 && (
                    <FormSelect
                        control={form.control}
                        name="streamField"
                        label="Streamed field"
                        description="Which response field streams token-by-token. The full answer is shown once the turn finishes."
                        options={streamFieldSelectOptions}
                        disabled={isStreaming}
                    />
                )}

                {parameterFields.fields.length > 0 && (
                    <TestParametersSection
                        control={form.control}
                        fields={parameterFields.fields}
                        isCollapsed={areParametersCollapsed}
                        onToggleCollapsed={() => setAreParametersCollapsed((collapsed) => !collapsed)}
                        disabled={isStreaming}
                    />
                )}

                {messages.length === 0 ? (
                    <div className="flex flex-1 flex-col items-center justify-center gap-2 py-8 text-center text-muted-foreground">
                        <MessageSquare className="size-8" aria-hidden />
                        <p className="text-sm">Ask the agent anything to see how it responds.</p>
                    </div>
                ) : (
                    <div className="flex flex-col gap-3">
                        {messages.map((message, index) => (
                            <TestMessage
                                key={message.id}
                                message={message}
                                // Only the in-flight agent turn (always the last message) shows the
                                // "generating" indicator above its answer.
                                isLoading={isStreaming && message.role === "agent" && index === messages.length - 1}
                            />
                        ))}
                    </div>
                )}
            </div>

            <form className="border-t p-4" onSubmit={withNestedSubmit(handleSend)}>
                <FormTextarea
                    control={form.control}
                    name="prompt"
                    placeholder="Ask the agent anything"
                    rows={3}
                    disabled={isStreaming}
                    onKeyDown={(event) => {
                        if (event.key === "Enter" && !event.shiftKey) {
                            event.preventDefault();
                            event.stopPropagation();
                            void handleSend();
                        }
                    }}
                />
                <div className="mt-2 flex justify-end gap-2">
                    <Button
                        type="button"
                        variant="ghost"
                        onClick={clearChat}
                        disabled={isStreaming || messages.length === 0}
                    >
                        <Trash2 className="size-4" aria-hidden />
                        Clear
                    </Button>
                    <Button type="submit" disabled={isStreaming || !prompt.trim()}>
                        {isStreaming ? <Spinner /> : <Send className="size-4" aria-hidden />}
                        Send
                    </Button>
                </div>
            </form>
        </div>
    );
}

// Operator-supplied values for the agent's declared parameters. Each value is required;
// the section can be collapsed to a one-line summary once filled to free up chat space.
function TestParametersSection({
    control,
    fields,
    isCollapsed,
    onToggleCollapsed,
    disabled,
}: {
    control: Control<TestFormData>;
    fields: { id: string; name: string }[];
    isCollapsed: boolean;
    onToggleCollapsed: () => void;
    disabled: boolean;
}) {
    const values = useWatch({ control, name: "parameters" });
    const summary = (values ?? [])
        .map((parameter) => `${parameter.name}: ${parameter.value?.trim() || "—"}`)
        .join(", ");

    return (
        <div className="grid gap-2 rounded-lg border bg-background p-3">
            <button
                type="button"
                className="flex w-full items-center justify-between gap-2 text-left"
                aria-expanded={!isCollapsed}
                onClick={onToggleCollapsed}
            >
                <span className="flex items-center gap-2 text-sm font-medium">
                    <Settings2 className="size-4 text-muted-foreground" aria-hidden />
                    Parameters
                </span>
                {isCollapsed ? (
                    <ChevronDown className="size-4 text-muted-foreground" aria-hidden />
                ) : (
                    <ChevronUp className="size-4 text-muted-foreground" aria-hidden />
                )}
            </button>
            {isCollapsed ? (
                <p className="truncate text-xs text-muted-foreground">{summary}</p>
            ) : (
                <div className="grid gap-2">
                    {fields.map((field, index) => (
                        <FormInput
                            key={field.id}
                            control={control}
                            name={`parameters.${index}.value`}
                            label={field.name}
                            placeholder={`Value for ${field.name}`}
                            disabled={disabled}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}

function TestMessage({ message, isLoading }: { message: ChatMessage; isLoading: boolean }) {
    if (message.role === "user") {
        return (
            <div className="ml-auto max-w-[85%] rounded-lg bg-primary px-3 py-2 text-sm whitespace-pre-wrap text-primary-foreground">
                {message.text}
            </div>
        );
    }

    if (message.role === "error") {
        return (
            <div className="mr-auto max-w-[85%] rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
                {message.text}
            </div>
        );
    }

    // Agent answers are always JSON, rendered in the read-only editor: live while the field
    // streams in, then the full structured answer once the turn finishes.
    return (
        <div className="mr-auto flex w-full gap-2">
            <Bot className="mt-2 size-4 shrink-0 text-muted-foreground" aria-hidden />
            <div className="min-w-0 flex-1">
                {isLoading && (
                    <div className="mb-1.5 flex items-center gap-2 text-xs text-muted-foreground">
                        <Spinner className="size-3" />
                        <span>Generating response…</span>
                    </div>
                )}
                {message.toolCalls && message.toolCalls.length > 0 && (
                    <div className="mb-2 grid gap-2">
                        {message.toolCalls.map((toolCall, index) => (
                            <TestQueryToolCall key={toolCall.id || index} toolCall={toolCall} />
                        ))}
                    </div>
                )}
                <div className="overflow-hidden rounded-lg border">
                    <AceEditor mode="json" value={message.json ?? ""} readOnly height="160px" maxHeight={400} />
                </div>
            </div>
        </div>
    );
}

function replaceMessage(messages: ChatMessage[], id: string, update: (message: ChatMessage) => ChatMessage) {
    return messages.map((message) => (message.id === id ? update(message) : message));
}

function toParameterRecord(parameters: TestFormData["parameters"]): Record<string, string> | null {
    const entries = parameters
        .map((parameter) => [parameter.name, parameter.value] as const)
        .filter(([name, value]) => name && value.trim() !== "");

    return entries.length > 0 ? Object.fromEntries(entries) : null;
}

// Builds the JSON shown while a single field streams in: the declared answer shape with the
// streamed field replaced by the text received so far, so the live answer reads as a real
// JSON object instead of a bare string. Falls back to a single-field object when the draft
// declares no output shape at all.
function buildStreamingJson(
    answerShape: Record<string, unknown> | null,
    streamField: string,
    streamedText: string,
): string {
    const answer: Record<string, unknown> = { ...(answerShape ?? {}) };
    const field = streamField || Object.keys(answer)[0] || "reply";
    answer[field] = streamedText;

    return JSON.stringify(answer, null, 4);
}

// Pretty-prints the structured answer for the JSON editor; null when there is nothing to show.
function toAnswerJson(answer: unknown): string | null {
    if (answer && typeof answer === "object" && Object.keys(answer).length > 0) {
        return JSON.stringify(answer, null, 4);
    }

    return null;
}

// The agent's declared output fields — the keys of the resolved output shape (see
// getOutputShape). Empty when the draft declares neither a sample nor a schema, which hides
// the "Streamed field" select and lets the server pick the default.
function getOutputFieldNames(sampleObject: string, outputSchema: string): string[] {
    return objectKeys(getOutputShape(sampleObject, outputSchema));
}

// The declared output shape used to skeleton the live answer: the sample response object when
// present (its instruction strings double as placeholders), otherwise a skeleton built from
// the Response JSON schema's `properties`. Null when the draft declares neither. The streamed
// field overwrites its slot, and the real answer replaces the whole thing once the turn ends.
function getOutputShape(sampleObject: string, outputSchema: string): Record<string, unknown> | null {
    const sample = parseJsonObject(sampleObject);
    if (sample && Object.keys(sample).length > 0) {
        return sample;
    }

    return schemaPropertiesToShape(parseJsonObject(outputSchema)?.properties);
}

// Turns a JSON schema's `properties` map into a placeholder object, one type-appropriate empty
// value per declared field (e.g. "" for strings, [] for arrays), so the streaming preview
// resembles the real response shape.
function schemaPropertiesToShape(properties: unknown): Record<string, unknown> | null {
    if (!properties || typeof properties !== "object" || Array.isArray(properties)) {
        return null;
    }

    const shape: Record<string, unknown> = {};
    for (const [field, definition] of Object.entries(properties as Record<string, unknown>)) {
        shape[field] = schemaTypePlaceholder(definition);
    }

    return Object.keys(shape).length > 0 ? shape : null;
}

function schemaTypePlaceholder(definition: unknown): unknown {
    const type = definition && typeof definition === "object" ? (definition as { type?: unknown }).type : undefined;

    switch (type) {
        case "array":
            return [];
        case "object":
            return {};
        case "number":
        case "integer":
            return 0;
        case "boolean":
            return false;
        default:
            return "";
    }
}

function parseJsonObject(json: string | null | undefined): Record<string, unknown> | null {
    if (!json || !json.trim()) {
        return null;
    }

    try {
        const parsed: unknown = JSON.parse(json);
        return parsed && typeof parsed === "object" && !Array.isArray(parsed)
            ? (parsed as Record<string, unknown>)
            : null;
    } catch {
        return null;
    }
}

function objectKeys(value: Record<string, unknown> | null): string[] {
    return value ? Object.keys(value) : [];
}
