import { useState } from "react";
import { Collapsible, CollapsibleContent } from "@/components/shadcn/ui/collapsible";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { cn } from "@/lib/utils";
import {
    describeConventions,
    getActivityHeadline,
    type PlanProgress,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import {
    ActivityRowLayout,
    ActivityRowList,
    CollapsibleHeader,
    DoneIcon,
    FailedIcon,
    PlannerWorkingCard,
    WarningIcon,
    WorkingIcon,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity-parts";
import { getRegisteredState } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import type { PlannerActivity, PlannerReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

/** Past this many rows a finished turn's activity starts folded, even as the latest. */
const MAX_UNFOLDED_ROWS = 8;

/**
 * A turn's tool activity folded into one summary. While the turn runs it lists what happened so far; once
 * it is done it collapses behind its headline, open only for a short latest turn until the operator decides.
 */
export function PlannerActivityCard({
    turn,
    progress,
    isLatest,
}: {
    turn: PlannerReplyTurn;
    progress: PlanProgress;
    isLatest: boolean;
}) {
    const [isOpenByOperator, setIsOpenByOperator] = useState<boolean | null>(null);
    const headline = getActivityHeadline(turn, progress);
    const rows = turn.activity.filter((activity) => activity.type !== "proposal");

    if (turn.status === "working") {
        return (
            <PlannerWorkingCard title={headline.title} detail={headline.detail}>
                {rows.map((activity, index) => (
                    <ActivityRow key={index} activity={activity} />
                ))}
                <ActivityRowLayout icon={<WorkingIcon />}>Working…</ActivityRowLayout>
            </PlannerWorkingCard>
        );
    }

    const isOpen = isOpenByOperator ?? (isLatest && rows.length <= MAX_UNFOLDED_ROWS);

    return (
        <Collapsible open={isOpen} onOpenChange={setIsOpenByOperator} className="rounded-lg border bg-card">
            <CollapsibleHeader isOpen={isOpen} className="px-3">
                <Text variant="label" as="span">
                    {headline.title}
                </Text>
                <Text
                    variant="muted"
                    as="span"
                    className={cn("ml-auto", headline.isDetailDestructive && "text-badge-destructive-fg")}
                >
                    {headline.detail}
                </Text>
            </CollapsibleHeader>
            <CollapsibleContent>
                <ActivityRowList>
                    {rows.map((activity, index) => (
                        <ActivityRow key={index} activity={activity} />
                    ))}
                </ActivityRowList>
            </CollapsibleContent>
        </Collapsible>
    );
}

function ActivityRow({ activity }: { activity: PlannerActivity }) {
    switch (activity.type) {
        case "conventions":
            return (
                <ActivityRowLayout icon={<DoneIcon />} detail={activity.notes}>
                    {describeConventions(activity)}
                </ActivityRowLayout>
            );

        case "note":
            return <ActivityRowLayout icon={<DoneIcon />}>{activity.text}</ActivityRowLayout>;

        case "lateError":
            return (
                <ActivityRowLayout icon={<WarningIcon />}>
                    The connection dropped after the planner finished: {activity.message}
                </ActivityRowLayout>
            );

        case "registered": {
            const verb = getRegisteredState(activity.version);

            return (
                <ActivityRowLayout
                    icon={activity.warningCount > 0 ? <WarningIcon /> : <DoneIcon />}
                    collection={activity.collection}
                >
                    {activity.warningCount > 0 ? `${verb} with ${formatCount(activity.warningCount, "warning")}` : verb}
                </ActivityRowLayout>
            );
        }

        case "rejected":
            return (
                <ActivityRowLayout icon={<FailedIcon />} collection={activity.collection}>
                    {activity.isChange ? "change rejected with" : "failed with"}{" "}
                    {formatCount(activity.errorCount, "error")}
                </ActivityRowLayout>
            );

        case "transientError":
            return (
                <ActivityRowLayout icon={<WarningIcon />} collection={activity.collection}>
                    retrying after {formatCount(activity.errors.length, "error")}
                </ActivityRowLayout>
            );

        case "removed":
            return <ActivityRowLayout icon={<DoneIcon />}>{activity.collection} removed</ActivityRowLayout>;

        case "proposal":
            return null;
    }
}
