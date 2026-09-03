import { useEffect, useRef, useState, type Ref } from "react";
import { Text } from "@/components/typography";
import { type Control, useFieldArray, useForm, useFormContext, useWatch } from "react-hook-form";
import { useParams } from "react-router";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { Bot, ChevronDown, ChevronUp, FlaskConical, MessageSquare, Send, Settings2, Trash2 } from "lucide-react";
import { Streamdown } from "streamdown";
import { api } from "@/api/api";
import type { AgentToolCall } from "@/api/custom-services/agent-stream";
import {
    AGENT_PARAMETER_TYPES,
    type AgentFormData,
} from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import type { WizardFooterComponentProps } from "@/components/form/wizard/form-wizard";
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
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormSwitch } from "@/components/form/form-switch";
import { FormTextarea } from "@/components/form/form-textarea";
import AceEditor from "@/components/ace-editor/ace-editor";
import { TestQueryToolCall } from "@/pages/setup/add-capability-wizard/steps/review/test-agent-tool-call";

// Footer action for the wizard's Review step: opens a sheet to chat with the draft agent.
// The button stays disabled until the draft has the minimum a test needs (name, system
// prompt, and an AI provider connection).
export function ReviewTestAgentButton({ isBusy }: WizardFooterComponentProps) {
    const { control } = useFormContext<AgentFormData>();
    const [name, systemPrompt, connectionStringName, actions] = useWatch({
        control,
        name: ["review.name", "review.systemPrompt", "connection.connectionStringName", "review.actions"],
    });
    const [isOpen, setIsOpen] = useState(false);

    const isReady = Boolean(name?.trim() && systemPrompt?.trim() && connectionStringName);
    const hasActions = (actions?.length ?? 0) > 0;

    return (
        <Sheet open={isOpen} onOpenChange={setIsOpen}>
            <SheetTrigger asChild>
                <Button
                    type="button"
                    variant="outline"
                    size="lg"
                    disabled={isBusy || !isReady}
                    title={
                        isReady ? undefined : "Add a name, system prompt, and AI provider connection before testing."
                    }
                >
                    <FlaskConical className="size-4" aria-hidden />
                    Test agent
                </Button>
            </SheetTrigger>
            <SheetContent
                className="flex w-full flex-col gap-0 sm:max-w-lg data-[side=right]:sm:max-w-lg"
                onOpenAutoFocus={(event) => event.preventDefault()}
            >
                <SheetHeader className="border-b">
                    <SheetTitle>Test agent</SheetTitle>
                    <SheetDescription>
                        Chat with the draft agent to check its answers. Each message runs a fresh, unsaved turn.
                        {hasActions &&
                            " Actions are excluded from test conversations — this agent is tested without them."}
                    </SheetDescription>
                </SheetHeader>
                <TestAgentPanel />
            </SheetContent>
        </Sheet>
    );
}

const NEWEST_PROMPT_TOP_GAP_IN_PX = 16;

let messageIdCounter = 0;
function nextMessageId(): string {
    messageIdCounter += 1;
    return `agent-test-message-${messageIdCounter}`;
}

// For agent answers, `text` is the primary markdown string (the streamed field live, then the
// final answer's string field) and `json` the structured answer (the sample response shape with
// the streamed field filling in, swapped for the full answer on `done`). `defaultView` picks
// which of the two a message opens on — markdown for the common single-string answer, JSON for
// anything more structured. `toolCalls` are the query tools the agent ran (filled on `done`).
// For user and error messages, `text` is the prompt / error alone.
type ChatMessage = {
    id: string;
    role: "user" | "agent" | "error";
    text: string;
    json?: string;
    defaultView?: AnswerView;
    toolCalls?: AgentToolCall[];
};

type AnswerView = "markdown" | "json";

const testParameterSchema = z
    .object({
        name: z.string(),
        value: z.string(),
        type: z.enum(AGENT_PARAMETER_TYPES),
        isSendToModel: z.boolean(),
    })
    .superRefine((parameter, ctx) => {
        const value = parameter.value.trim();
        if (parameter.type === "Null") {
            return;
        }

        if (!value) {
            ctx.addIssue({ code: "custom", message: "Value is required", path: ["value"] });
            return;
        }

        if (parameter.type === "Number" && !Number.isFinite(Number(value))) {
            ctx.addIssue({ code: "custom", message: "Enter a valid number", path: ["value"] });
        } else if (parameter.type === "Boolean" && !isBooleanToken(value)) {
            ctx.addIssue({ code: "custom", message: "Select true or false", path: ["value"] });
        } else if (parameter.type.startsWith("ArrayOf") && !isValidParameterArray(value, parameter.type)) {
            ctx.addIssue({ code: "custom", message: `Enter a valid ${parameter.type} JSON array`, path: ["value"] });
        }
    });

const testFormSchema = z.object({
    prompt: z.string(),
    // Which output field streams token-by-token; empty lets the server pick the first field.
    streamField: z.string(),
    parameters: z.array(testParameterSchema),
});

type TestFormData = z.infer<typeof testFormSchema>;

function TestAgentPanel() {
    const { slug = "" } = useParams();
    const wizardForm = useFormContext<AgentFormData>();
    const [messages, setMessages] = useState<ChatMessage[]>([]);
    const [isStreaming, setIsStreaming] = useState(false);
    const [areParametersCollapsed, setAreParametersCollapsed] = useState(false);
    const scrollRef = useRef<HTMLDivElement>(null);
    const newestPromptRef = useRef<HTMLDivElement>(null);
    const newestPromptId = messages.findLast((message) => message.role === "user")?.id;

    // Keep the newest prompt near the top as its answer streams in, so the answer reads from its
    // first line instead of the view chasing the bottom. Scrolling this container directly, rather
    // than scrollIntoView, keeps the sheet itself from scrolling along.
    useEffect(() => {
        const scrollArea = scrollRef.current;
        const newestPrompt = newestPromptRef.current;
        if (!scrollArea || !newestPrompt) {
            return;
        }

        scrollArea.scrollTop +=
            newestPrompt.getBoundingClientRect().top -
            scrollArea.getBoundingClientRect().top -
            NEWEST_PROMPT_TOP_GAP_IN_PX;
    }, [messages]);

    // Re-read the draft's output shape live from the wizard form (it stays mounted behind the
    // sheet), so the "Streamed field" options and the streaming preview track edits to the
    // sample object / schema instead of a stale open-time snapshot.
    const [sampleObject, outputSchema] = useWatch({
        control: wizardForm.control,
        name: ["review.sampleObject", "review.outputSchema"],
    });
    const answerShape = getOutputShape(sampleObject, outputSchema);
    // Only string-typed fields can stream as text, so they alone populate the select.
    const streamFieldOptions = getStreamableFieldNames(answerShape);

    // Aborts the in-flight stream when the panel unmounts (the sheet closes) so the request stops
    // and no state update fires afterwards.
    const abortControllerRef = useRef<AbortController | null>(null);
    useEffect(() => () => abortControllerRef.current?.abort(), []);

    const form = useForm<TestFormData>({
        resolver: zodResolver(testFormSchema),
        defaultValues: {
            prompt: "",
            // Default to the first streamable field — the server's own convention when unset.
            streamField: streamFieldOptions[0] ?? "",
            // The agent's declared parameters, ready for the operator to fill in values. RavenDB
            // requires every declared parameter value for a top-level conversation; policy only
            // controls parameter generation when this agent is used as a sub-agent.
            parameters: wizardForm.getValues("review.parameters").map((parameter) => ({
                name: parameter.name,
                value: "",
                type: parameter.type,
                isSendToModel: parameter.isSendToModel,
            })),
        },
    });
    const parameterFields = useFieldArray({ control: form.control, name: "parameters" });
    const prompt = useWatch({ control: form.control, name: "prompt" });

    // The prompt box is disabled while a turn streams, which drops focus. Take it back once the
    // turn ends, unless the operator has moved to a control in the transcript.
    useEffect(() => {
        if (isStreaming || scrollRef.current?.contains(document.activeElement)) {
            return;
        }

        form.setFocus("prompt");
    }, [form, isStreaming]);

    const streamFieldSelectOptions: FormSelectOption<string>[] = streamFieldOptions.map((name) => ({
        value: name,
        label: name,
    }));

    async function send(values: TestFormData) {
        const trimmedPrompt = values.prompt.trim();
        if (!trimmedPrompt || isStreaming) {
            return;
        }

        // Re-read the wizard form so the test always runs the latest draft. The streamed field
        // fills into `answerShape` (derived above from the same live form values), so the live
        // answer reads as a real JSON object while it streams in.
        const wizardValues = wizardForm.getValues();
        // The draft test runs an unsaved configuration, so it has no action bindings to call and
        // would leave any action the model triggers unanswered. Test the agent without them.
        const configuration = { ...buildAgentConfigurationPayload(wizardValues), actions: [] };
        const parameters = toParameterRecord(values.parameters);
        // An edit to the sample/schema may have dropped the selected field; fall back to the
        // first streamable one so the value sent matches the current options.
        const streamField = streamFieldOptions.includes(values.streamField)
            ? values.streamField
            : (streamFieldOptions[0] ?? "");

        const agentMessageId = nextMessageId();
        setMessages((previous) => [
            ...previous,
            { id: nextMessageId(), role: "user", text: trimmedPrompt },
            { id: agentMessageId, role: "agent", text: "", json: buildStreamingJson(answerShape, streamField, "") },
        ]);
        form.setValue("prompt", "");
        setIsStreaming(true);

        const abortController = new AbortController();
        abortControllerRef.current = abortController;

        let streamedText = "";
        try {
            for await (const event of api.services.agentTest.stream(
                slug,
                {
                    prompt: trimmedPrompt,
                    configuration,
                    parameters,
                    streamField: streamField || null,
                },
                abortController.signal,
            )) {
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
                    // text/JSON where the server returned no structured answer. Attach the query
                    // tools the agent ran so the transcript can show them above the answer.
                    const answer = event.fullAnswer ?? event.answer;
                    const json = toAnswerJson(answer) ?? buildStreamingJson(answerShape, streamField, streamedText);
                    const text = getPrimaryAnswerText(answer, streamField) ?? streamedText;
                    const toolCalls = event.toolCalls ?? [];
                    setMessages((previous) =>
                        replaceMessage(previous, agentMessageId, (message) => ({
                            ...message,
                            text,
                            json,
                            defaultView: getDefaultAnswerView(answer, text),
                            toolCalls,
                        })),
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
            // The panel closed mid-stream (the unmount cleanup aborted) — the component is gone,
            // so there's nothing to surface.
            if (abortController.signal.aborted) {
                return;
            }

            const message = error instanceof Error ? error.message : "Agent test failed.";
            setMessages((previous) =>
                replaceMessage(previous, agentMessageId, () => ({ id: agentMessageId, role: "error", text: message })),
            );
        } finally {
            abortControllerRef.current = null;
            setIsStreaming(false);
        }
    }

    function clearChat() {
        setMessages([]);
        form.setValue("prompt", "");
    }

    // Build and run the submit handler at event time (not during render), so `send` — which
    // manages the abort-controller ref — isn't treated as a render-time ref access. On a blocked
    // send, reveal the parameters so the validation errors aren't hidden behind a collapsed section.
    function handleSend() {
        return form.handleSubmit(send, () => setAreParametersCollapsed(false))();
    }

    return (
        <div className="flex min-h-0 flex-1 flex-col">
            <div ref={scrollRef} className="flex flex-1 flex-col gap-4 overflow-y-auto p-4">
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

                {parameterFields.fields.some((field) => field.type !== "Null") && (
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
                                ref={message.id === newestPromptId ? newestPromptRef : undefined}
                                message={message}
                                // Only the in-flight agent turn (always the last message) shows the
                                // "generating" indicator above its answer.
                                isLoading={isStreaming && message.role === "agent" && index === messages.length - 1}
                            />
                        ))}
                    </div>
                )}
            </div>

            <form
                className="border-t p-4"
                // Inlined (rather than withNestedSubmit) so the ref-touching handleSend stays in an
                // event handler. stopPropagation keeps this nested submit off the outer wizard form.
                onSubmit={(event) => {
                    event.preventDefault();
                    event.stopPropagation();
                    handleSend();
                }}
            >
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
                            handleSend();
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

// Operator-supplied values for the agent's declared parameters. Null parameters have no editable
// value; all others are required for a top-level test conversation. The section can be collapsed
// to a one-line summary once filled to free up chat space.
function TestParametersSection({
    control,
    fields,
    isCollapsed,
    onToggleCollapsed,
    disabled,
}: {
    control: Control<TestFormData>;
    fields: { id: string; name: string; type: TestFormData["parameters"][number]["type"] }[];
    isCollapsed: boolean;
    onToggleCollapsed: () => void;
    disabled: boolean;
}) {
    const values = useWatch({ control, name: "parameters" });
    const summary = (values ?? [])
        .filter((parameter) => parameter.type !== "Null")
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
                <Text variant="label" as="span" className="flex items-center gap-2">
                    <Settings2 className="size-4 text-muted-foreground" aria-hidden />
                    Parameters
                </Text>
                {isCollapsed ? (
                    <ChevronDown className="size-4 text-muted-foreground" aria-hidden />
                ) : (
                    <ChevronUp className="size-4 text-muted-foreground" aria-hidden />
                )}
            </button>
            {isCollapsed ? (
                <Text variant="caption" className="truncate">
                    {summary}
                </Text>
            ) : (
                <div className="grid gap-2">
                    {fields.map((field, index) => {
                        if (field.type === "Null") {
                            return null;
                        }

                        return (
                            <div key={field.id} className="grid gap-2 rounded-md border p-3">
                                <div className="flex items-center justify-between gap-3">
                                    <Text variant="label" as="span" className="truncate" title={field.name}>
                                        {field.name}
                                    </Text>
                                    <FormSwitch
                                        control={control}
                                        name={`parameters.${index}.isSendToModel`}
                                        label="Send to model"
                                        disabled={disabled}
                                    />
                                </div>
                                {field.type === "Boolean" ? (
                                    <FormSelect
                                        control={control}
                                        name={`parameters.${index}.value`}
                                        label="Value"
                                        placeholder="Select true or false"
                                        options={BOOLEAN_PARAMETER_OPTIONS}
                                        disabled={disabled}
                                    />
                                ) : (
                                    <FormInput
                                        control={control}
                                        name={`parameters.${index}.value`}
                                        label="Value"
                                        placeholder={getParameterPlaceholder(field.name, field.type)}
                                        inputMode={field.type === "Number" ? "decimal" : undefined}
                                        disabled={disabled}
                                    />
                                )}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

function TestMessage({
    message,
    isLoading,
    ref,
}: {
    message: ChatMessage;
    isLoading: boolean;
    ref?: Ref<HTMLDivElement>;
}) {
    if (message.role === "user") {
        return (
            <div
                ref={ref}
                className="ml-auto max-w-[85%] rounded-lg bg-primary px-3 py-2 text-sm whitespace-pre-wrap text-primary-foreground"
            >
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

    return <AgentAnswer message={message} isLoading={isLoading} />;
}

// An agent answer opens on formatted markdown of its primary string (the common answer shape is
// a single string field) with a per-message toggle to the raw JSON; answers with more structure
// open on JSON instead (see getDefaultAnswerView). While waiting for the first streamed token
// only the "Generating" indicator shows — no empty answer box.
function AgentAnswer({ message, isLoading }: { message: ChatMessage; isLoading: boolean }) {
    const [selectedView, setSelectedView] = useState<AnswerView | null>(null);
    const hasMarkdown = message.text.trim() !== "";
    const view = hasMarkdown ? (selectedView ?? message.defaultView ?? "markdown") : "json";
    const isAwaitingFirstChunk = isLoading && !hasMarkdown;

    return (
        <div className="mr-auto flex w-full gap-2">
            <Bot className="mt-2 size-4 shrink-0 text-muted-foreground" aria-hidden />
            <div className="min-w-0 flex-1">
                {isLoading && (
                    <Text variant="caption" as="div" className="mt-2 mb-1.5 flex items-center gap-2">
                        <Spinner className="size-3" />
                        <span>Generating response…</span>
                    </Text>
                )}
                {message.toolCalls && message.toolCalls.length > 0 && (
                    <div className="mb-2 grid gap-2">
                        {message.toolCalls.map((toolCall, index) => (
                            <TestQueryToolCall key={toolCall.id || index} toolCall={toolCall} />
                        ))}
                    </div>
                )}
                {!isAwaitingFirstChunk && (
                    <div className="grid gap-1.5">
                        {hasMarkdown && (
                            <ToggleGroup
                                type="single"
                                variant="outline"
                                size="sm"
                                spacing={0}
                                className="justify-self-end"
                                value={view}
                                onValueChange={(value) => {
                                    if (value === "markdown" || value === "json") {
                                        setSelectedView(value);
                                    }
                                }}
                            >
                                <ToggleGroupItem value="markdown" aria-label="Show formatted answer">
                                    Markdown
                                </ToggleGroupItem>
                                <ToggleGroupItem value="json" aria-label="Show raw JSON answer">
                                    JSON
                                </ToggleGroupItem>
                            </ToggleGroup>
                        )}
                        {view === "markdown" ? (
                            <div className="rounded-lg border bg-background px-3 py-2 text-sm">
                                <Streamdown>{message.text}</Streamdown>
                            </div>
                        ) : (
                            <div className="overflow-hidden rounded-lg border">
                                <AceEditor
                                    mode="json"
                                    value={message.json ?? ""}
                                    readOnly
                                    height="160px"
                                    maxHeight={400}
                                    actions={[{ component: <AceEditor.FullScreenAction /> }]}
                                />
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}

function replaceMessage(messages: ChatMessage[], id: string, update: (message: ChatMessage) => ChatMessage) {
    return messages.map((message) => (message.id === id ? update(message) : message));
}

const BOOLEAN_PARAMETER_OPTIONS: FormSelectOption<string>[] = [
    { value: "true", label: "True" },
    { value: "false", label: "False" },
];

// Mirrors RavenDB Studio's createParametersDto: convert the form text to the parameter's declared
// JSON type and send the per-test SendToModel value alongside it.
function toParameterRecord(
    parameters: TestFormData["parameters"],
): Record<string, { value: unknown; sendToModel: boolean }> | null {
    const entries = parameters
        .filter((parameter) => parameter.name)
        .map(
            (parameter) =>
                [
                    parameter.name,
                    {
                        value: mapParameterValueToType(parameter.value, parameter.type),
                        sendToModel: parameter.isSendToModel,
                    },
                ] as const,
        );

    return entries.length > 0 ? Object.fromEntries(entries) : null;
}

function mapParameterValueToType(value: string, type: TestFormData["parameters"][number]["type"]): unknown {
    switch (type) {
        case "Number":
            return Number(value);
        case "Boolean":
            return value.trim().toLowerCase() === "true";
        case "ArrayOfString":
            return requireParameterArray(value);
        case "ArrayOfNumber":
            return requireParameterArray(value).map((item) => Number(item));
        case "ArrayOfBoolean":
            return requireParameterArray(value).map((item) =>
                typeof item === "boolean" ? item : String(item).trim().toLowerCase() === "true",
            );
        case "Null":
            return null;
        case "Default":
        case "String":
            return value;
    }
}

function requireParameterArray(value: string): unknown[] {
    const parsed: unknown = JSON.parse(value);
    if (!Array.isArray(parsed)) {
        throw new Error("Expected an agent parameter value in JSON array format.");
    }
    return parsed;
}

function isBooleanToken(value: string): boolean {
    const normalized = value.trim().toLowerCase();
    return normalized === "true" || normalized === "false";
}

function isValidParameterArray(value: string, type: TestFormData["parameters"][number]["type"]): boolean {
    try {
        const parsed: unknown = JSON.parse(value);
        if (!Array.isArray(parsed)) {
            return false;
        }

        switch (type) {
            case "ArrayOfString":
                return parsed.every((item) => typeof item === "string");
            case "ArrayOfNumber":
                return parsed.every(isFiniteNumberToken);
            case "ArrayOfBoolean":
                return parsed.every(
                    (item) => typeof item === "boolean" || (typeof item === "string" && isBooleanToken(item)),
                );
            default:
                return false;
        }
    } catch {
        return false;
    }
}

function isFiniteNumberToken(value: unknown): boolean {
    if (typeof value === "number") {
        return Number.isFinite(value);
    }
    return typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value));
}

function getParameterPlaceholder(name: string, type: TestFormData["parameters"][number]["type"]): string {
    switch (type) {
        case "ArrayOfString":
            return `["value1", "value2"] for ${name}`;
        case "ArrayOfNumber":
            return `[1, 2] for ${name}`;
        case "ArrayOfBoolean":
            return `[true, false] for ${name}`;
        case "Number":
            return `Number for ${name}`;
        default:
            return `Value for ${name}`;
    }
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
    // Prefer the chosen field, then the first string-typed field (a non-string slot can't hold
    // streamed text), then any field, then a single "reply" key when the draft declares no shape.
    const field = streamField || firstStringKey(answer) || Object.keys(answer)[0] || "reply";
    answer[field] = streamedText;

    return JSON.stringify(answer, null, 2);
}

function firstStringKey(answer: Record<string, unknown>): string | undefined {
    return Object.keys(answer).find((key) => typeof answer[key] === "string");
}

// The string the markdown view renders: the answer itself when it is a bare string, else the
// streamed field when it holds text, else the first non-empty string field. Null when the
// answer has no string content at all.
function getPrimaryAnswerText(answer: unknown, streamField: string): string | null {
    if (typeof answer === "string") {
        return answer.trim() ? answer : null;
    }

    if (!answer || typeof answer !== "object" || Array.isArray(answer)) {
        return null;
    }

    const record = answer as Record<string, unknown>;
    const preferred = record[streamField];
    if (typeof preferred === "string" && preferred.trim()) {
        return preferred;
    }

    const firstString = Object.values(record).find((value) => typeof value === "string" && value.trim());
    return typeof firstString === "string" ? firstString : null;
}

// Markdown is the default view only for the common simple shape — an answer that is effectively
// a single string. Anything with more populated fields would lose information in the markdown
// view (it shows only the primary string), so it opens on JSON instead.
function getDefaultAnswerView(answer: unknown, markdownText: string): AnswerView {
    if (!markdownText.trim()) {
        return "json";
    }

    if (!answer || typeof answer !== "object" || Array.isArray(answer)) {
        return "markdown";
    }

    const populatedValues = Object.values(answer).filter(hasAnswerContent);
    return populatedValues.length === 1 && typeof populatedValues[0] === "string" ? "markdown" : "json";
}

function hasAnswerContent(value: unknown): boolean {
    if (value == null) {
        return false;
    }
    if (typeof value === "string") {
        return value.trim() !== "";
    }
    if (Array.isArray(value)) {
        return value.length > 0;
    }
    if (typeof value === "object") {
        return Object.keys(value).length > 0;
    }
    return true;
}

// Pretty-prints the structured answer for the JSON editor; null when there is nothing to show.
function toAnswerJson(answer: unknown): string | null {
    if (answer && typeof answer === "object" && Object.keys(answer).length > 0) {
        return JSON.stringify(answer, null, 2);
    }

    return null;
}

// The output fields that can stream as text: the string-typed keys of the resolved output shape
// (see getOutputShape). A non-string field (array/object/number) can't hold a streamed string, so
// it's excluded from the "Streamed field" select and never used as the default. Empty when the
// draft declares no string output field, which hides the select and lets the server pick.
function getStreamableFieldNames(shape: Record<string, unknown> | null): string[] {
    return shape ? Object.keys(shape).filter((key) => typeof shape[key] === "string") : [];
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
