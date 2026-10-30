import { Heading, Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { AreaCollectionCard } from "@/pages/setup/add-app-wizard/steps/map/area/area-collection-card";
import { AreaSwitcher } from "@/pages/setup/add-app-wizard/steps/map/area/area-switcher";
import { BuildButton } from "@/pages/setup/add-app-wizard/steps/map/plan/build-button";
import { PlannerBreadcrumb } from "@/pages/setup/add-app-wizard/steps/map/planner-breadcrumb";
import {
    EditsDisabledNote,
    PlannerUnsavedChanges,
} from "@/pages/setup/add-app-wizard/steps/map/planner-unsaved-changes";
import type { Plan, PlanArea } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { useBuildBlockReason, usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";

export function AreaView({ plan, area }: { plan: Plan; area: PlanArea }) {
    const actions = usePlannerActions();
    const buildBlockReason = useBuildBlockReason();
    const canBuild = area.pendingCollections.length > 0 && !area.isBuilding;
    const collectionCount = area.collections.filter((collection) => collection.state !== "removed").length;

    return (
        <div className="mx-auto flex w-full max-w-280 flex-col gap-4 px-4 py-5 2xl:px-7">
            <PlannerBreadcrumb items={[{ label: "Plan", onClick: actions.openPlan }]}>
                <AreaSwitcher plan={plan} current={area} />
            </PlannerBreadcrumb>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
                <Heading as="h2" variant="section">
                    {area.name}
                </Heading>
                <Text as="span" variant="caption">
                    {formatCount(collectionCount, "collection")}
                </Text>
                {canBuild && (
                    <span className="ml-auto">
                        <BuildButton
                            label={area.isStarted ? "Build the rest" : "Build"}
                            blockReason={buildBlockReason}
                            isPrimary
                            onBuild={() => actions.buildArea(area)}
                        />
                    </span>
                )}
            </div>
            <EditsDisabledNote />
            <PlannerUnsavedChanges />
            <div className="flex flex-col gap-2.5">
                {area.collections.map((collection) => (
                    <AreaCollectionCard key={collection.name} collection={collection} />
                ))}
            </div>
        </div>
    );
}
