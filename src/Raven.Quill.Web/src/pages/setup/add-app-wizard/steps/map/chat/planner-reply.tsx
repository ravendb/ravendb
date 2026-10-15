import { CircleX } from "lucide-react";
import { Streamdown } from "streamdown";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Text } from "@/components/typography";
import { formatCount, joinList, upperFirst } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { getTurnOutcomes, hasCollectionOutcomes } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { PlannerActivityCard } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity-card";
import { DoneIcon, WorkingIcon } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity-parts";
import { PlannerBuildSuggestedCard } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-build-suggested-card";
import { PlannerMessage, PlannerPlanCard } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-message";
import { PlannerOpeningProgress } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-opening-progress";
import { PlanWarning } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-collection-card";
import { isBuilt, type PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { isRegistered } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import type { PlannerActivity, PlannerReplyTurn } from "@/pages/setup/add-app-wizard/steps/map/session/planner-types";

type ProposalActivity = Extract<PlannerActivity, { type: "proposal" }>;

export function PlannerReply({
    turn,
    isLatest,
    collections,
}: {
    turn: PlannerReplyTurn;
    isLatest: boolean;
    collections: PlanCollection[];
}) {
    const builtCount = collections.filter(isBuilt).length;
    const collectionCount = collections.length;
    const tableCount = useSetupWizardStore((state) => state.plannerSelectedTables?.length ?? 0);
    const openPlannerPanel = useSetupWizardStore((state) => state.openPlannerPanel);

    if (turn.isOpening && turn.status === "working") {
        return <PlannerOpeningProgress turn={turn} />;
    }

    const proposal = turn.activity.findLast((activity) => activity.type === "proposal");
    const outcomes = getTurnOutcomes(turn.activity);
    const hasActivity = turn.activity.some((activity) => activity.type !== "proposal");
    const isPlanReady = collectionCount > 0 && builtCount === collectionCount;
    const hasReadSchema = turn.isOpening && (turn.activity.length > 0 || turn.reply !== null);

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
                {proposal && <ProposalCard proposal={proposal} onView={() => openPlannerPanel()} />}
                {hasActivity && (
                    <PlannerActivityCard
                        turn={turn}
                        progress={{ builtCount, totalCount: collectionCount }}
                        isLatest={isLatest}
                    />
                )}
                {turn.status === "working" && !hasActivity && (
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
                    <PlanWarning key={index}>
                        <strong>Gap.</strong> {gap}
                    </PlanWarning>
                ))}
                {turn.error && (
                    <Alert variant="destructive">
                        <CircleX aria-hidden="true" />
                        <AlertDescription>{turn.error}</AlertDescription>
                    </Alert>
                )}
                {turn.status === "stopped" && <Text variant="muted">Stopped before finishing.</Text>}
                {isLatest && <PlannerBuildSuggestedCard collections={collections} />}
                {isLatest && turn.status === "done" && hasCollectionOutcomes(outcomes) && isPlanReady && (
                    <PlannerPlanCard
                        title={`Plan · ${formatCount(collectionCount, "collection")} built`}
                        detail={
                            outcomes.updated.length > 0
                                ? `${joinList(outcomes.updated)} updated · ready to apply`
                                : "Ready to apply"
                        }
                        actionLabel="Open plan"
                        onAction={() => openPlannerPanel()}
                    />
                )}
            </PlannerMessage>
        </>
    );
}

function ProposalCard({ proposal, onView }: { proposal: ProposalActivity; onView: () => void }) {
    const plannerCollections = useSetupWizardStore((state) => state.plannerCollections);
    const builtCount = proposal.collections.filter((name) => {
        const collection = plannerCollections[name];

        return collection !== undefined && isRegistered(collection);
    }).length;
    const collections = formatCount(proposal.collections.length, "collection");
    const progress = builtCount === 0 ? "nothing built yet" : `${builtCount} of ${proposal.collections.length} built`;

    return (
        <PlannerPlanCard
            title={
                proposal.areaCount > 0
                    ? `Plan · ${collections} in ${formatCount(proposal.areaCount, "area")}`
                    : `Plan · ${collections}`
            }
            detail={upperFirst(
                proposal.droppedCount > 0
                    ? `${formatCount(proposal.droppedCount, "table")} not migrated · ${progress}`
                    : progress,
            )}
            actionLabel="View"
            onAction={onView}
        />
    );
}
