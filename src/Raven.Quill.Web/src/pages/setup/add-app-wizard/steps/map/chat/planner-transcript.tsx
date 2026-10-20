import { useEffect, useEffectEvent, useState } from "react";
import { Text } from "@/components/typography";
import { useStickToBottom } from "@/hooks/use-stick-to-bottom";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlannerMentionChip } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-mention-chip";
import { PlannerReply } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-reply";
import { usePlannerChatWidthClass } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { getLatestReply } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type {
    PlannerAnswersTurn,
    PlannerPrompt,
    PlannerPromptTurn,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const BUBBLE_CLASS = "flex max-w-[85%] flex-col gap-2 self-end rounded-xl bg-muted px-3.5 py-2.5";

export function PlannerTranscript({ collections }: { collections: PlanCollection[] }) {
    const turns = useSetupWizardStore((state) => state.plannerTurns);
    const widthClass = usePlannerChatWidthClass();
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
                "flex min-h-0 flex-1 flex-col overflow-y-auto px-6 pt-4 pb-3 tall:pt-6 tall:pb-4",
                isScrolledDown && "mask-t-from-[calc(100%-3rem)]",
            )}
            onScroll={(event) => {
                onScroll(event);
                setIsScrolledDown(event.currentTarget.scrollTop > 0);
            }}
        >
            <div ref={contentRef} className={cn("mx-auto flex w-full flex-col gap-4", widthClass)}>
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
                                    turn={turn}
                                    isLatest={turn.id === latestReplyId}
                                    collections={collections}
                                />
                            );
                    }
                })}
            </div>
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
                {turn.isSkipped
                    ? "You skipped the questions, so the planner goes with its recommendations"
                    : "Your answers"}
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
            {turn.instructions && <PromptContent prompt={turn.instructions} />}
        </div>
    );
}
