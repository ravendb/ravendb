import { useState } from "react";
import { formatCount } from "@/lib/format";
import {
    describeConventions,
    getActivityHeadline,
    getActivityRows,
    getRegisteredOutcome,
    type PlanProgress,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import {
    ActivityGroup,
    ActivityRowLayout,
    DoneIcon,
    FailedIcon,
    PlannerWorkingActivity,
    WarningIcon,
    WorkingIcon,
} from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity-parts";
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
    const rows = getActivityRows(turn.activity);

    if (turn.status === "working") {
        return (
            <PlannerWorkingActivity title={headline.title} detail={headline.detail}>
                {rows.map((activity, index) => (
                    <ActivityRow key={index} activity={activity} />
                ))}
                <ActivityRowLayout icon={<WorkingIcon />}>Working…</ActivityRowLayout>
            </PlannerWorkingActivity>
        );
    }

    const isOpen = isOpenByOperator ?? (isLatest && rows.length <= MAX_UNFOLDED_ROWS);

    return (
        <ActivityGroup
            icon={headline.isDetailDestructive ? <FailedIcon /> : <DoneIcon />}
            title={headline.title}
            detail={headline.detail}
            isDetailDestructive={headline.isDetailDestructive}
            isOpen={isOpen}
            onOpenChange={setIsOpenByOperator}
        >
            {rows.map((activity, index) => (
                <ActivityRow key={index} activity={activity} />
            ))}
        </ActivityGroup>
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
            const verb = getRegisteredOutcome(activity.version);

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
            return (
                <ActivityRowLayout icon={<DoneIcon />} detail={activity.reason}>
                    {activity.collection} removed
                </ActivityRowLayout>
            );

        case "proposal":
            return null;
    }
}
