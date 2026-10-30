import type { ReactNode } from "react";
import { ArrowRight, CircleAlert, CircleCheck, CircleDashed, CircleHelp, CirclePause } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Text } from "@/components/typography";
import { formatCount, joinList } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { BuildButton } from "@/pages/setup/add-app-wizard/steps/map/plan/build-button";
import { countBuilt, type Plan, type PlanArea } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { getChatPhase, getPendingQuestions } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { useBuildBlockReason, usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";

type StatusTone = "working" | "ask" | "problem" | "next" | "idle" | "done";

const TONE_ICONS: Record<StatusTone, ReactNode> = {
    working: <Spinner className="text-primary-strong" />,
    ask: <CircleHelp className="size-4 text-primary-strong" />,
    problem: <CircleAlert className="size-4 text-badge-destructive-fg" />,
    next: <CircleDashed className="size-4 text-muted-foreground" />,
    idle: <CirclePause className="size-4 text-muted-foreground" />,
    done: <CircleCheck className="size-4 text-badge-success-fg" />,
};

export function PlanStatusLine({ plan }: { plan: Plan }) {
    const actions = usePlannerActions();
    const buildBlockReason = useBuildBlockReason();
    const proposalChat = useSetupWizardStore((state) => state.plannerProposalChat);
    const hasProposal = useSetupWizardStore((state) => state.plannerProposal !== null);
    const tableCount = useSetupWizardStore((state) => state.plannerSelectedTables?.length ?? 0);
    const catalogName = useSetupWizardStore((state) => state.discoverResult?.catalogName);
    const setPlannerChatOpen = useSetupWizardStore((state) => state.setPlannerChatOpen);

    const phase = getChatPhase(proposalChat);
    const collections = plan.collections.filter((collection) => collection.state !== "removed");
    const builtCount = countBuilt(collections);
    const built = `${builtCount} of ${collections.length} built`;
    const areasToBuild = plan.areas.filter((area) => area.pendingCollections.length > 0 && !area.isBuilding);
    const buildingAreas = plan.areas.filter((area) => area.isBuilding);
    const failed = collections.filter((collection) => collection.state === "failed");
    const buildFirst = (area: PlanArea | undefined, isPrimary: boolean) =>
        area && (
            <BuildButton
                label={`Build ${area.name}`}
                blockReason={buildBlockReason}
                isPrimary={isPrimary}
                onBuild={() => actions.buildAndOpenArea(area)}
            />
        );

    if (phase === "working") {
        return hasProposal ? (
            <StatusLine tone="working" title="Updating the plan" text="The planner is working on the plan." />
        ) : (
            <StatusLine
                tone="working"
                title={`Reading ${formatCount(tableCount, "table")}${catalogName ? ` from ${catalogName}` : ""}`}
                text="The planner reads names, columns, types and keys, never your data. Collections show up here as it suggests them."
            />
        );
    }

    if (!hasProposal) {
        return <StatusLine tone="idle" title="The planner stopped" text="It stopped before it proposed a plan." />;
    }

    if (phase === "awaitingAnswers") {
        const questionCount = getPendingQuestions(proposalChat).length;

        return (
            <StatusLine
                tone="ask"
                title={
                    questionCount === 1 ? "The planner has a question" : `The planner has ${questionCount} questions`
                }
                text="Answer them in the chat. The questions settle the conventions before any area starts."
                action={
                    <Button size="sm" onClick={() => setPlannerChatOpen(true)}>
                        Answer in the chat
                        <ArrowRight data-icon="inline-end" />
                    </Button>
                }
            />
        );
    }

    if (buildingAreas.length > 0) {
        return (
            <StatusLine
                tone="working"
                title={`Building ${describeAreas(buildingAreas)}`}
                text="Open an area to watch it build. Other areas keep going while you look around."
            />
        );
    }

    if (failed.length > 0) {
        const failedArea = failed[0].area;

        return (
            <StatusLine
                tone="problem"
                title={`${built}, ${joinList(failed.map((collection) => collection.name))} failed`}
                text={`Fix it in its area, or carry on without it${areasToBuild.length > 0 ? " and build the next area" : ""}. Only built collections go to the app.`}
                action={
                    <>
                        <Button variant="outline" size="sm" onClick={() => actions.openArea(failedArea)}>
                            Open {failedArea}
                            <ArrowRight data-icon="inline-end" />
                        </Button>
                        {buildFirst(areasToBuild[0], false)}
                    </>
                }
            />
        );
    }

    if (builtCount === 0) {
        return (
            <StatusLine
                tone="next"
                title="The plan is ready"
                text="Build an area to map its collections. Each area builds in its own chat, and several can build at once."
                action={buildFirst(areasToBuild[0], true)}
            />
        );
    }

    if (areasToBuild.length > 0) {
        const verb = areasToBuild.length === 1 ? "is" : "are";
        const pronoun = areasToBuild.length === 1 ? "it" : "them";

        return (
            <StatusLine
                tone="next"
                title={built}
                text={`${describeAreas(areasToBuild)} ${verb} not built yet. Build ${pronoun}, or create the app without ${pronoun}.`}
                action={buildFirst(areasToBuild[0], false)}
            />
        );
    }

    return (
        <StatusLine
            tone="done"
            title="Ready to create the app"
            text={`${formatCount(builtCount, "collection")} ${builtCount === 1 ? "goes" : "go"} to the app.`}
        />
    );
}

function describeAreas(areas: PlanArea[]): string {
    return areas.length <= 2 ? joinList(areas.map((area) => area.name)) : formatCount(areas.length, "area");
}

function StatusLine({
    tone,
    title,
    text,
    action,
}: {
    tone: StatusTone;
    title: string;
    text: string;
    action?: ReactNode;
}) {
    return (
        <div
            role="status"
            className={cn(
                "flex flex-wrap items-center gap-x-3 gap-y-2 rounded-lg bg-foreground/5 py-2.5 pr-2.5 pl-3",
                tone === "problem" && "bg-badge-destructive-bg",
                tone === "done" && "bg-badge-success-bg",
            )}
        >
            <span className="flex size-5 shrink-0 items-center justify-center" aria-hidden="true">
                {TONE_ICONS[tone]}
            </span>
            <div className="flex min-w-0 flex-[1_1_15rem] flex-col">
                <Text as="span" variant="label">
                    {title}
                </Text>
                <Text as="span" variant="muted">
                    {text}
                </Text>
            </div>
            {action && <div className="flex flex-wrap gap-2">{action}</div>}
        </div>
    );
}
