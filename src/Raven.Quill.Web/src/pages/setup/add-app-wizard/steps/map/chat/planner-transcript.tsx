import { useEffect, useEffectEvent, useState } from "react";
import { Button } from "@/components/shadcn/ui/button";
import { Text } from "@/components/typography";
import { useStickToBottom } from "@/hooks/use-stick-to-bottom";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { getSuggestedReplies, getTurnOutcomes } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { PlannerMentionChip } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-mention-chip";
import { PlannerReply } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-reply";
import { useCanSendMessage } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { Plan } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { getLatestReply, selectChat } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";
import type {
    PlannerAnswersTurn,
    PlannerChatId,
    PlannerPrompt,
    PlannerPromptTurn,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const BUBBLE_CLASS = "flex max-w-[85%] flex-col gap-2 self-end rounded-xl bg-muted px-3.5 py-2.5";

export function PlannerTranscript({ chatId, plan }: { chatId: PlannerChatId; plan: Plan }) {
    const turns = useSetupWizardStore((state) => selectChat(state, chatId).turns);
    const hasProposal = useSetupWizardStore((state) => state.plannerProposal !== null);
    const { scrollRef, contentRef, onScroll, follow } = useStickToBottom<HTMLDivElement, HTMLDivElement>();
    const [isScrolledDown, setIsScrolledDown] = useState(false);
    const latestReplyId = getLatestReply(turns)?.id;
    const followNewTurn = useEffectEvent(follow);

    // A turn the operator just started is followed, wherever they had scrolled to.
    useEffect(() => {
        followNewTurn();
    }, [latestReplyId]);

    return (
        <div
            ref={scrollRef}
            className={cn(
                "flex min-h-0 flex-1 flex-col overflow-y-auto px-4 pt-5 pb-3",
                isScrolledDown && "mask-t-from-[calc(100%-3rem)]",
            )}
            onScroll={(event) => {
                onScroll(event);
                setIsScrolledDown(event.currentTarget.scrollTop > 0);
            }}
        >
            <div ref={contentRef} className="flex w-full flex-col gap-4">
                {turns.length === 0 && (hasProposal || chatId !== null) && (
                    <Text variant="muted" className="rounded-md border border-dashed px-2.5 py-2">
                        {chatId === null
                            ? "The conversation from before is not shown here. The plan is up to date, and the planner remembers what was said."
                            : "This area's conversation from before is not shown here. Its collections are up to date."}
                    </Text>
                )}
                {turns.map((turn) => {
                    switch (turn.kind) {
                        case "prompt":
                            return <PromptBubble key={turn.id} turn={turn} />;
                        case "answers":
                            return <AnswersBubble key={turn.id} turn={turn} />;
                        case "planner":
                            return (
                                <PlannerReply
                                    key={turn.id}
                                    chatId={chatId}
                                    turn={turn}
                                    isLatest={turn.id === latestReplyId}
                                    plan={plan}
                                />
                            );
                    }
                })}
                {chatId !== null && <SuggestedReplies chatId={chatId} plan={plan} />}
            </div>
        </div>
    );
}

function SuggestedReplies({ chatId, plan }: { chatId: string; plan: Plan }) {
    const session = usePlannerSession();
    const canSendMessage = useCanSendMessage(chatId);
    const latestReply = useSetupWizardStore((state) => getLatestReply(selectChat(state, chatId).turns));

    if (latestReply?.status !== "done") {
        return null;
    }

    const collections = plan.areas.find((area) => area.name === chatId)?.collections ?? [];
    const outcomes = getTurnOutcomes(latestReply.activity);
    const hasProblems = (name: string) =>
        collections.some((collection) => collection.name === name && collection.problems.length > 0);
    const replies = getSuggestedReplies({
        failed: outcomes.failed.filter(hasProblems),
        kept: outcomes.kept.filter(hasProblems),
    });

    if (replies.length === 0) {
        return null;
    }

    return (
        <div role="group" aria-label="Suggested replies" className="flex flex-wrap gap-2">
            {replies.map((reply) => (
                <Button
                    key={reply.label}
                    variant="outline"
                    size="sm"
                    disabled={!canSendMessage}
                    onClick={() => void session.askArea(chatId, { text: reply.prompt, mentions: [] })}
                >
                    {reply.label}
                </Button>
            ))}
        </div>
    );
}

function PromptBubble({ turn }: { turn: PlannerPromptTurn }) {
    return (
        <div className={BUBBLE_CLASS}>
            <PromptContent prompt={turn} />
        </div>
    );
}

function PromptContent({ prompt }: { prompt: PlannerPrompt }) {
    return (
        <>
            {prompt.mentions.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                    {prompt.mentions.map((collection) => (
                        <PlannerMentionChip key={collection} collection={collection} />
                    ))}
                </div>
            )}
            {prompt.text && <Text className="whitespace-pre-wrap">{prompt.text}</Text>}
        </>
    );
}

function AnswersBubble({ turn }: { turn: PlannerAnswersTurn }) {
    return (
        <div className={BUBBLE_CLASS}>
            <Text variant="caption" as="span">
                Your answers
            </Text>
            <dl className="flex flex-col gap-2">
                {turn.answers.map(({ question, answer }, index) => (
                    <div key={index} className="flex flex-col">
                        <dt>
                            <Text variant="muted" as="span">
                                {question}
                            </Text>
                        </dt>
                        <dd>
                            <Text as="span">{answer}</Text>
                        </dd>
                    </div>
                ))}
            </dl>
        </div>
    );
}
