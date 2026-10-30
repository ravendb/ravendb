import { useLayoutEffect, useRef, useState } from "react";
import { PanelRightClose, Sparkle } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Heading, Text } from "@/components/typography";
import { formatCount, joinList } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { describeWork } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { PlannerComposer } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-composer";
import {
    PlannerQuestions,
    PlannerQuestionsReminder,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-questions";
import { PlannerTranscript } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-transcript";
import {
    useChatPhase,
    usePlannerWorkspace,
    useShownChatId,
} from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { Plan, PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import {
    getLatestReply,
    getPendingQuestions,
    selectChat,
    selectHasUnsavedEdits,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";

/** Several chats can work at once, and switching to another chat never stops the one left. */
export function PlannerChat({ plan }: { plan: Plan }) {
    const chatId = useShownChatId();
    const viewedArea = useSetupWizardStore((state) => state.plannerView.area);
    const setPlannerChatOpen = useSetupWizardStore((state) => state.setPlannerChatOpen);
    const hasUnsavedEdits = useSetupWizardStore(selectHasUnsavedEdits);
    const isAreaWaiting = viewedArea !== null && chatId === null;
    const collections =
        chatId === null ? plan.collections : (plan.areas.find((area) => area.name === chatId)?.collections ?? []);

    return (
        <aside aria-label="Chat" className="flex h-full min-h-0 flex-col bg-surface2">
            <div className="flex min-h-13 items-center gap-2.5 border-b py-2 pr-2 pl-3.5">
                <span
                    aria-hidden="true"
                    className="flex size-6 shrink-0 items-center justify-center rounded-full bg-badge-default-bg text-primary-strong"
                >
                    <Sparkle className="size-3.5" />
                </span>
                <div className="flex min-w-0 flex-1 flex-col">
                    <Heading as="h2" variant="label" className="truncate">
                        {chatId === null ? "Plan chat" : `${chatId} chat`}
                    </Heading>
                    <ChatStatus chatId={chatId} collections={collections} />
                </div>
                <Tooltip>
                    <TooltipTrigger asChild>
                        <Button
                            variant="ghost"
                            size="icon-sm"
                            className="text-muted-foreground"
                            aria-label="Hide the chat"
                            onClick={() => setPlannerChatOpen(false)}
                        >
                            <PanelRightClose aria-hidden="true" />
                        </Button>
                    </TooltipTrigger>
                    <TooltipContent>Hide the chat</TooltipContent>
                </Tooltip>
            </div>
            <PlannerTranscript key={chatId ?? ""} chatId={chatId} plan={plan} />
            <div className="flex flex-col gap-2 px-3 pb-3">
                {isAreaWaiting && (
                    <Text variant="caption">
                        The {viewedArea} chat starts when you build this area. Until then, the planner answers here.
                    </Text>
                )}
                {hasUnsavedEdits && (
                    <Text variant="caption">
                        Save or discard your changes to message{" "}
                        {chatId === null ? "the planner" : `the ${chatId} agent`}.
                    </Text>
                )}
                <ChatBottom key={chatId ?? ""} chatId={chatId} collections={collections} />
            </div>
        </aside>
    );
}

function ChatStatus({ chatId, collections }: { chatId: string | null; collections: PlanCollection[] }) {
    const phase = useChatPhase(chatId);
    const latestReply = useSetupWizardStore((state) => getLatestReply(selectChat(state, chatId).turns));
    const hasProposal = useSetupWizardStore((state) => state.plannerProposal !== null);
    const failed = collections.filter((collection) => collection.state === "failed");

    const status = (() => {
        switch (phase) {
            case "working":
                if (chatId !== null) {
                    return latestReply ? describeWork(latestReply) : "Working";
                }

                return hasProposal ? "Updating the plan" : "Working on a first plan";
            case "awaitingAnswers":
                return "Waiting for your answers";
            default:
                if (latestReply?.status === "stopped") {
                    return "Stopped";
                }

                if (chatId === null) {
                    return hasProposal ? "Plan ready" : "Ready";
                }

                if (failed.length > 0) {
                    const names =
                        failed.length <= 2
                            ? joinList(failed.map((collection) => collection.name))
                            : formatCount(failed.length, "collection");

                    return `${names} ${failed.length === 1 ? "needs" : "need"} your attention`;
                }

                return "Ready";
        }
    })();

    return (
        <Text as="span" variant="caption">
            {status}
        </Text>
    );
}

/**
 * The planner's questions take the composer's place until they are answered, or until the operator closes
 * them to write a message instead. New questions in a later reply show again.
 */
function ChatBottom({ chatId, collections }: { chatId: string | null; collections: PlanCollection[] }) {
    const { composerRef } = usePlannerWorkspace();
    const questions = useSetupWizardStore((state) => getPendingQuestions(selectChat(state, chatId)));
    const latestReply = useSetupWizardStore((state) => getLatestReply(selectChat(state, chatId).turns));
    const [closedQuestionsReplyId, setClosedQuestionsReplyId] = useState<string | null>(null);
    const questionsRef = useRef<HTMLElement>(null);
    const latestReplyId = latestReply?.id ?? null;
    const hasPendingQuestions = questions.length > 0;
    const isShowingQuestions = hasPendingQuestions && closedQuestionsReplyId !== latestReplyId;
    const wasShowingQuestionsRef = useRef(isShowingQuestions);

    // When the two trade places, the element that had focus is gone; focus follows to whatever replaced it.
    useLayoutEffect(() => {
        if (wasShowingQuestionsRef.current === isShowingQuestions) {
            return;
        }

        wasShowingQuestionsRef.current = isShowingQuestions;

        if (document.activeElement === document.body) {
            (isShowingQuestions ? questionsRef.current : composerRef.current)?.focus();
        }
    }, [isShowingQuestions, composerRef]);

    if (isShowingQuestions) {
        return (
            <PlannerQuestions
                key={latestReplyId}
                ref={questionsRef}
                chatId={chatId}
                questions={questions}
                onClose={() => setClosedQuestionsReplyId(latestReplyId)}
            />
        );
    }

    return (
        <>
            {hasPendingQuestions && (
                <PlannerQuestionsReminder
                    chatId={chatId}
                    questionCount={questions.length}
                    onOpen={() => setClosedQuestionsReplyId(null)}
                />
            )}
            <PlannerComposer chatId={chatId} collections={collections} />
        </>
    );
}
