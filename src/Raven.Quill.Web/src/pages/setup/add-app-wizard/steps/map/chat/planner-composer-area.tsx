import { useLayoutEffect, useRef } from "react";
import { RotateCcw } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import {
    getSuggestedReplies,
    getTurnOutcomes,
    type TurnOutcomes,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { PlannerComposer } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-composer";
import { PlannerQuestions } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-questions";
import {
    useCanAskPlanner,
    usePlannerChatWidthClass,
    usePlannerWorkspace,
} from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { getLatestReply, selectPendingQuestions } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

export function PlannerComposerArea({ collections }: { collections: PlanCollection[] }) {
    const { composerRef } = usePlannerWorkspace();
    const questions = useSetupWizardStore(selectPendingQuestions);
    const latestReply = useSetupWizardStore((state) => getLatestReply(state.plannerTurns));
    const widthClass = usePlannerChatWidthClass();
    const questionsRef = useRef<HTMLElement>(null);
    const isAwaitingAnswers = questions.length > 0;
    const wasAwaitingAnswersRef = useRef(isAwaitingAnswers);

    // When the two trade places, the element that had focus is gone; focus follows to whatever replaced it.
    useLayoutEffect(() => {
        if (wasAwaitingAnswersRef.current === isAwaitingAnswers) {
            return;
        }

        wasAwaitingAnswersRef.current = isAwaitingAnswers;

        if (document.activeElement === document.body) {
            (isAwaitingAnswers ? questionsRef.current : composerRef.current)?.focus();
        }
    }, [isAwaitingAnswers, composerRef]);

    if (isAwaitingAnswers) {
        return (
            <div className="px-6 pb-4 2xl:pb-5">
                <PlannerQuestions
                    key={latestReply?.id}
                    ref={questionsRef}
                    questions={questions}
                    className={widthClass}
                />
            </div>
        );
    }

    const outcomes = latestReply?.status === "done" ? getTurnOutcomes(latestReply.activity) : null;

    return (
        <div className="px-6 pb-4 2xl:pb-5">
            <div className={cn("mx-auto flex w-full flex-col gap-2.5", widthClass)}>
                <SuggestedReplies outcomes={outcomes} collections={collections} />
                <PlannerComposer collections={collections} />
            </div>
        </div>
    );
}

function SuggestedReplies({ outcomes, collections }: { outcomes: TurnOutcomes | null; collections: PlanCollection[] }) {
    const session = usePlannerSession();
    const canAsk = useCanAskPlanner();
    const dismissPlannerRejections = useSetupWizardStore((state) => state.dismissPlannerRejections);
    const hasErrors = (name: string) =>
        collections.some((collection) => collection.name === name && collection.errors.length > 0);
    const replies = outcomes
        ? getSuggestedReplies({ failed: outcomes.failed.filter(hasErrors), kept: outcomes.kept.filter(hasErrors) })
        : [];

    if (replies.length === 0) {
        return null;
    }

    return (
        <div role="group" aria-label="Suggested replies" className="flex flex-wrap gap-2">
            {replies.map((reply) => (
                <Button
                    key={reply.label}
                    variant="outline"
                    className="rounded-full px-3"
                    disabled={!canAsk}
                    onClick={() => {
                        dismissPlannerRejections(reply.keeps);
                        void session.ask({ text: reply.prompt, mentions: [] });
                    }}
                >
                    {reply.isRetry && <RotateCcw className="size-3" aria-hidden="true" />}
                    {reply.label}
                </Button>
            ))}
        </div>
    );
}
