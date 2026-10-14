import { Hammer } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { getCollectionsToBuildFirst } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { useCanAskPlanner } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { composeBuildCollectionsPrompt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import { selectPlannerPhase } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

export function PlannerBuildSuggestedCard({ collections }: { collections: PlanCollection[] }) {
    const session = usePlannerSession();
    const canAsk = useCanAskPlanner();
    const isReady = useSetupWizardStore((state) => selectPlannerPhase(state) === "ready");
    const collectionsToBuild = getCollectionsToBuildFirst(collections);

    if (!isReady || collectionsToBuild.length === 0) {
        return null;
    }

    return (
        <div className="flex flex-wrap items-center gap-x-4 gap-y-3 rounded-lg border border-brand-600/30 bg-primary/6 py-3 pr-3 pl-3.5 dark:border-primary/30 dark:bg-primary/7">
            <div className="flex min-w-60 flex-1 flex-col gap-0.5">
                <Text as="span" variant="label" className="text-primary-strong">
                    Next step
                </Text>
                <Text variant="muted" as="span">
                    Build the {formatCount(collectionsToBuild.length, "suggested collection")}, then review them or ask
                    for changes.
                </Text>
            </div>
            <Button
                disabled={!canAsk}
                onClick={() =>
                    void session.ask({ text: composeBuildCollectionsPrompt(collectionsToBuild), mentions: [] })
                }
            >
                <Hammer aria-hidden="true" />
                Build suggested collections
            </Button>
        </div>
    );
}
