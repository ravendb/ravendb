import { CircleX } from "lucide-react";
import { Streamdown } from "streamdown";
import { ErrorDetails } from "@/components/data/error-details";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { hasActivityToShow } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { PlannerActivityCard } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity-card";
import { DoneIcon, WorkingIcon } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity-parts";
import { PlannerMessage } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-message";
import { PlannerOpeningProgress } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-opening-progress";
import { PlannerRetryButton } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-retry-button";
import { countBuilt, type Plan } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import type { PlannerChatId, PlannerReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

const MAX_INLINE_ERROR_LENGTH = 200;

export function PlannerReply({
    chatId,
    turn,
    isLatest,
    plan,
}: {
    chatId: PlannerChatId;
    turn: PlannerReplyTurn;
    isLatest: boolean;
    plan: Plan;
}) {
    const tableCount = useSetupWizardStore((state) => state.plannerSelectedTables?.length ?? 0);
    const area = chatId === null ? null : plan.areas.find((candidate) => candidate.name === chatId);
    const collections = (area?.collections ?? plan.collections).filter((collection) => collection.state !== "removed");

    if (chatId === null && turn.isOpening && turn.status === "working") {
        return <PlannerOpeningProgress turn={turn} />;
    }

    const hasActivity = hasActivityToShow(turn);
    const hasReadSchema = chatId === null && turn.isOpening && (turn.activity.length > 0 || turn.reply !== null);
    const isAreaOpening = chatId !== null && turn.isOpening;

    return (
        <>
            {hasReadSchema && (
                <div className="flex items-center gap-2">
                    <DoneIcon />
                    <Text variant="muted" as="span">
                        Read the schema of {formatCount(tableCount, "table")}
                    </Text>
                </div>
            )}
            <PlannerMessage>
                {isAreaOpening && (
                    <Text>
                        I&apos;m building the <strong>{chatId}</strong> from the plan. Nothing goes further until you
                        create the app.
                    </Text>
                )}
                {(hasActivity || isAreaOpening) && (
                    <PlannerActivityCard
                        turn={turn}
                        progress={{ builtCount: countBuilt(collections), totalCount: collections.length }}
                        isLatest={isLatest}
                    />
                )}
                {turn.status === "working" && !hasActivity && !isAreaOpening && (
                    <div className="flex items-center gap-2">
                        <WorkingIcon />
                        <Text variant="muted" as="span">
                            Working…
                        </Text>
                    </div>
                )}
                {turn.reply && (
                    <Text as="div">
                        <Streamdown>{turn.reply}</Streamdown>
                    </Text>
                )}
                {turn.gaps.map((gap, index) => (
                    <Text key={index} role="note" className="border-l-2 border-warning py-0.5 pl-3">
                        <strong>Gap.</strong> {gap}
                    </Text>
                ))}
                {turn.error && <TurnError error={turn.error} />}
                {turn.status === "failed" && isLatest && <PlannerRetryButton chatId={chatId} area={area ?? null} />}
                {turn.status === "stopped" && <Text variant="muted">Stopped before finishing.</Text>}
            </PlannerMessage>
        </>
    );
}

/** An agent's error can carry a whole server stack trace, so its first line shows and the rest folds away. */
function TurnError({ error }: { error: string }) {
    const message = error.trim();
    const summary = message.split("\n", 1)[0];
    const hasDetails = summary !== message || summary.length > MAX_INLINE_ERROR_LENGTH;

    return (
        <Alert variant="destructive">
            <CircleX aria-hidden="true" />
            <AlertDescription className="grid gap-2">
                <span className={cn("wrap-anywhere", hasDetails && "line-clamp-3")}>{summary}</span>
                {hasDetails && <ErrorDetails details={message} />}
            </AlertDescription>
        </Alert>
    );
}
