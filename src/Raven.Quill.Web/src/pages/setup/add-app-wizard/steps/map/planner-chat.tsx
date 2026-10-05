import { useEffect, useRef, useState } from "react";
import { SendHorizontal, Square } from "lucide-react";
import { Streamdown } from "streamdown";
import { Button } from "@/components/shadcn/ui/button";
import { Textarea } from "@/components/shadcn/ui/textarea";
import { Text } from "@/components/typography";
import { useSetupWizardStore, type PlannerMessage } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlannerQuestions } from "@/pages/setup/add-app-wizard/steps/map/planner-questions";

/** Matches the assistant panel: close enough to the bottom and the view keeps following the stream. */
const STICK_TO_BOTTOM_THRESHOLD_PX = 48;

export function PlannerChat({
    isStreaming,
    canAsk,
    onAsk,
    onStop,
}: {
    isStreaming: boolean;
    canAsk: boolean;
    onAsk: (prompt: string) => void;
    onStop: () => void;
}) {
    const messages = useSetupWizardStore((state) => state.plannerMessages);
    const questions = useSetupWizardStore((state) => state.plannerQuestions);
    const [prompt, setPrompt] = useState("");
    const scrollRef = useRef<HTMLDivElement>(null);
    const isFollowingStreamRef = useRef(true);

    useEffect(() => {
        const scrollArea = scrollRef.current;
        if (scrollArea && isFollowingStreamRef.current) {
            scrollArea.scrollTop = scrollArea.scrollHeight;
        }
    }, [messages]);

    const send = () => {
        const text = prompt.trim();
        if (!text || !canAsk || isStreaming) {
            return;
        }

        setPrompt("");
        onAsk(text);
    };

    return (
        <div className="flex h-full min-h-0 flex-col">
            <div
                ref={scrollRef}
                className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-3"
                onScroll={(event) => {
                    const { scrollTop, scrollHeight, clientHeight } = event.currentTarget;
                    isFollowingStreamRef.current =
                        scrollHeight - scrollTop - clientHeight <= STICK_TO_BOTTOM_THRESHOLD_PX;
                }}
            >
                {messages.length === 0 ? (
                    <Text variant="muted">
                        Start a session to have the planner read your schema and propose a document model.
                    </Text>
                ) : (
                    messages.map((message) => <PlannerMessageItem key={message.id} message={message} />)
                )}
                {isStreaming && (
                    <Text variant="muted" className="animate-pulse">
                        Thinking&hellip;
                    </Text>
                )}
            </div>

            {questions.length > 0 && !isStreaming ? (
                <PlannerQuestions questions={questions} onAnswer={onAsk} />
            ) : (
                <div className="border-t p-3">
                    <div className="relative">
                        <Textarea
                            rows={3}
                            value={prompt}
                            onChange={(event) => setPrompt(event.target.value)}
                            onKeyDown={(event) => {
                                if (event.key === "Enter" && !event.shiftKey) {
                                    event.preventDefault();
                                    send();
                                }
                            }}
                            disabled={!canAsk}
                            placeholder={
                                canAsk
                                    ? "Tell the planner which collections to build, or ask for a change."
                                    : "Start a session first."
                            }
                            className="pr-12"
                        />
                        <Button
                            type="button"
                            size="icon"
                            variant={isStreaming ? "secondary" : "default"}
                            className="absolute right-2 bottom-2"
                            onClick={isStreaming ? onStop : send}
                            disabled={!canAsk || (!isStreaming && !prompt.trim())}
                            aria-label={isStreaming ? "Stop" : "Send"}
                        >
                            {isStreaming ? <Square aria-hidden /> : <SendHorizontal aria-hidden />}
                        </Button>
                    </div>
                </div>
            )}
        </div>
    );
}

function PlannerMessageItem({ message }: { message: PlannerMessage }) {
    if (message.role === "user") {
        return (
            <div className="ml-auto max-w-[85%] rounded-lg bg-primary px-3 py-2 text-sm whitespace-pre-wrap text-primary-foreground">
                {message.text}
            </div>
        );
    }

    if (message.role === "error") {
        return (
            <div className="max-w-[85%] rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
                {message.text}
            </div>
        );
    }

    // A tool call is a marker, not a turn: the detail of what it did is in the results pane.
    if (message.role === "tool") {
        return (
            <Text variant="caption" as="div" className="border-l-2 pl-2">
                {message.text}
            </Text>
        );
    }

    return (
        <div className="rounded-lg py-1 text-sm">
            <Streamdown>{message.text}</Streamdown>
        </div>
    );
}
