import { useLayoutEffect, useRef, useState, type RefObject } from "react";
import { PanelRight, RotateCcw } from "lucide-react";
import { CountBadge } from "@/components/data/count-badge";
import { Button } from "@/components/shadcn/ui/button";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { selectIsBuildingCollections } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { PlannerComposerArea } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-composer-area";
import { PlannerTranscript } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-transcript";
import { PlanPanel } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-panel";
import { PlannerFrame } from "@/pages/setup/add-app-wizard/steps/map/planner-frame";
import {
    PlannerWorkspaceContext,
    usePlannerWorkspace,
} from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import {
    countByState,
    countChangedSince,
    isBuilt,
    type PlanCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import {
    selectPendingQuestions,
    selectPlannerPhase,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { usePlanCollections } from "@/pages/setup/add-app-wizard/steps/map/session/use-plan-collections";

const PLAN_BESIDE_CHAT_MIN_WIDTH_REM = 46;

/**
 * The chat is the main surface; the plan opens beside it once the planner has proposed one. Where the
 * two do not fit side by side, or while the planner waits on its questions, the plan only opens when
 * the operator asks for it.
 */
export function PlannerWorkspace({ isBusy, onChangeSelection }: { isBusy: boolean; onChangeSelection: () => void }) {
    const collections = usePlanCollections();
    const hasProposal = useSetupWizardStore((state) => state.plannerProposal !== null);
    const panel = useSetupWizardStore((state) => state.plannerPanel);
    const hasPendingQuestions = useSetupWizardStore((state) => selectPendingQuestions(state).length > 0);
    const workspaceRef = useRef<HTMLDivElement>(null);
    const composerRef = useRef<HTMLTextAreaElement>(null);
    const isNarrow = useIsNarrowerThan(workspaceRef, PLAN_BESIDE_CHAT_MIN_WIDTH_REM);

    const hasPlan = hasProposal || collections.length > 0;
    const isPanelOpen = hasPlan && (panel === "open" || (panel === "auto" && !isNarrow && !hasPendingQuestions));

    return (
        <PlannerWorkspaceContext.Provider
            value={{ composerRef, isBusy, isPanelOpen, isNarrow, changeSelection: onChangeSelection }}
        >
            <PlannerFrame
                actions={
                    <>
                        <StartOverButton />
                        <PlanToggle
                            collections={collections}
                            hasPlan={hasPlan}
                            isPanelOpen={isPanelOpen}
                            isPanelShownWhenFree={panel === "auto" && !isNarrow}
                        />
                    </>
                }
            >
                <div ref={workspaceRef} className="relative flex min-h-0 flex-1">
                    <div className="flex min-w-0 flex-1 flex-col">
                        <PlannerTranscript collections={collections} />
                        <PlannerComposerArea collections={collections} />
                    </div>
                    {isPanelOpen && <PlanPanel collections={collections} />}
                </div>
            </PlannerFrame>
        </PlannerWorkspaceContext.Provider>
    );
}

function StartOverButton() {
    const { isBusy } = usePlannerWorkspace();
    const phase = useSetupWizardStore(selectPlannerPhase);
    const startPlannerOver = useSetupWizardStore((state) => state.startPlannerOver);
    const isWorking = phase === "opening" || phase === "working";

    return (
        <ConfirmDialog
            trigger={
                <Button
                    variant="ghost"
                    size="icon"
                    className="text-muted-foreground"
                    aria-label="Start over"
                    title="Start over"
                    disabled={isWorking || isBusy}
                >
                    <RotateCcw aria-hidden="true" />
                </Button>
            }
            variant="destructive"
            icon={null}
            title="Start over?"
            description="This clears the conversation and the plan, including the built collections. The tables you selected stay as they are."
            confirmLabel="Start over"
            onConfirm={startPlannerOver}
        />
    );
}

/**
 * The dot flags what changed since the operator last saw the plan. A plan that opens by itself whenever the
 * planner is not waiting on questions shows every change as it happens, so it never has unseen changes.
 */
function PlanToggle({
    collections,
    hasPlan,
    isPanelOpen,
    isPanelShownWhenFree,
}: {
    collections: PlanCollection[];
    hasPlan: boolean;
    isPanelOpen: boolean;
    isPanelShownWhenFree: boolean;
}) {
    const seenRevisions = useSetupWizardStore((state) => state.plannerSeenRevisions);
    const proposal = useSetupWizardStore((state) => state.plannerProposal);
    const plannerCollections = useSetupWizardStore((state) => state.plannerCollections);
    const removedCollections = useSetupWizardStore((state) => state.plannerRemovedCollections);
    const isBuilding = useSetupWizardStore(selectIsBuildingCollections);
    const openPlannerPanel = useSetupWizardStore((state) => state.openPlannerPanel);
    const closePlannerPanel = useSetupWizardStore((state) => state.closePlannerPanel);

    if (!hasPlan) {
        return (
            <Button
                variant="ghost"
                aria-disabled="true"
                title="The plan appears here once the planner proposes one"
                className="cursor-not-allowed text-muted-foreground opacity-60 hover:bg-transparent hover:text-muted-foreground"
            >
                <PanelRight data-icon="inline-start" />
                Plan
            </Button>
        );
    }

    const changedCount =
        isPanelOpen || isPanelShownWhenFree
            ? 0
            : countChangedSince(seenRevisions, {
                  plannerProposal: proposal,
                  plannerCollections,
                  plannerRemovedCollections: removedCollections,
              });
    const areaCount = proposal?.areas.length ?? 0;
    const builtCount = collections.filter(isBuilt).length;
    const failedCount = countByState(collections).failed;
    const hasBuildStarted = isBuilding || builtCount > 0 || failedCount > 0;
    const summary = describePlan({ collections, builtCount, failedCount, areaCount, isBuilding });

    return (
        <Button
            variant={isPanelOpen ? "secondary" : "outline"}
            aria-pressed={isPanelOpen}
            aria-label={
                changedCount > 0
                    ? `Plan, ${summary}, ${formatCount(changedCount, "collection")} changed`
                    : `Plan, ${summary}`
            }
            title={summary}
            className="relative px-2"
            onClick={() => (isPanelOpen ? closePlannerPanel() : openPlannerPanel())}
        >
            <PanelRight aria-hidden="true" />
            Plan
            <CountBadge variant={isPanelOpen ? "outline" : "secondary"}>
                {hasBuildStarted ? `${builtCount}/${collections.length} built` : collections.length}
                {failedCount > 0 && <span className="text-badge-destructive-fg">· {failedCount} failed</span>}
            </CountBadge>
            {changedCount > 0 && (
                <span
                    aria-hidden="true"
                    className="absolute -top-0.75 -right-0.75 size-2.25 rounded-full border-2 border-background bg-primary"
                />
            )}
        </Button>
    );
}

function describePlan({
    collections,
    builtCount,
    failedCount,
    areaCount,
    isBuilding,
}: {
    collections: PlanCollection[];
    builtCount: number;
    failedCount: number;
    areaCount: number;
    isBuilding: boolean;
}): string {
    const built = `${builtCount} of ${collections.length} built`;

    if (isBuilding) {
        return `Building · ${built}`;
    }

    if (builtCount === 0 && failedCount === 0) {
        const suggested = `${collections.length} suggested ${collections.length === 1 ? "collection" : "collections"}`;

        return areaCount > 0 ? `${suggested} in ${formatCount(areaCount, "area")}` : suggested;
    }

    return failedCount > 0 ? `${built} · ${failedCount} failed` : built;
}

/** Layout, not render, knows how wide the workspace is; it is measured before the first paint. */
function useIsNarrowerThan(ref: RefObject<HTMLElement | null>, widthRem: number): boolean {
    const [isNarrower, setIsNarrower] = useState(false);

    useLayoutEffect(() => {
        const element = ref.current;

        if (!element) {
            return;
        }

        const widthPx = widthRem * parseFloat(getComputedStyle(document.documentElement).fontSize);
        const measure = () => setIsNarrower(element.getBoundingClientRect().width < widthPx);
        const observer = new ResizeObserver(measure);

        measure();
        observer.observe(element);

        return () => observer.disconnect();
    }, [ref, widthRem]);

    return isNarrower;
}
