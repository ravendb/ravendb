// import { useLayoutEffect, useRef } from "react";
import { CountBadge } from "@/components/data/count-badge";
import { Progress } from "@/components/shadcn/ui/progress";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlanCollectionDetails } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-collection-details";
import { PlanCollectionsTab } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-collections-tab";
import { PlanFilterBar } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-filter-bar";
import { PlanTablesTab } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-tables-tab";
import { selectIsBuildingCollections } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-activity";
import { isBuilt, type PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { usePlannerWorkspace } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import {
    usePlanFilters,
    usePlanTables,
    type PlanTab,
} from "@/pages/setup/add-app-wizard/steps/map/panel/use-plan-filters";

const TAB_CLASS =
    "h-7 rounded-md px-2.5 text-muted-foreground data-[state=active]:bg-muted data-[state=active]:text-foreground dark:data-[state=active]:border-transparent dark:data-[state=active]:bg-muted group-data-[variant=default]/tabs-list:data-[state=active]:shadow-none";

/**
 * A collection's details cover the overview rather than replace it, so the filters and the scroll
 * position are where they were when the operator comes back.
 */
export function PlanPanel({ collections }: { collections: PlanCollection[] }) {
    const { isNarrow } = usePlannerWorkspace();
    const detailsName = useSetupWizardStore((state) => state.plannerDetailsCollection);
    const details = collections.find((collection) => collection.name === detailsName);

    return (
        <aside
            aria-label={details ? `${details.name} in the plan` : "Plan"}
            className={cn(
                "relative flex w-105 min-w-0 shrink-0 flex-col border-l bg-surface2",
                isNarrow && "absolute inset-y-0 right-0 z-10 max-w-full shadow-xl",
            )}
        >
            <div inert={details !== undefined} className="flex min-h-0 flex-1 flex-col">
                <PlanOverview collections={collections} />
            </div>
            {details && (
                <div className="absolute inset-0 flex flex-col bg-surface2">
                    <PlanCollectionDetails key={details.name} collection={details} />
                </div>
            )}
        </aside>
    );
}

function PlanOverview({ collections }: { collections: PlanCollection[] }) {
    const isBuilding = useSetupWizardStore(selectIsBuildingCollections);
    const tables = usePlanTables(collections);
    const filters = usePlanFilters(collections, tables);

    const builtCount = collections.filter(isBuilt).length;
    const isProgressShown = isBuilding && builtCount < collections.length;

    return (
        <Tabs
            value={filters.tab}
            onValueChange={(value) => filters.setTab(value as PlanTab)}
            className="min-h-0 flex-1 gap-0"
        >
            <div className="flex flex-col gap-2 border-b pt-3 pr-2 pb-3 pl-4">
                {isProgressShown && (
                    <Progress
                        aria-label="Collections built"
                        value={(builtCount / collections.length) * 100}
                        getValueLabel={() => `${builtCount} of ${collections.length} built`}
                        className="mr-2"
                    />
                )}

                <TabsList
                    aria-label="Plan views"
                    className="mr-2 grid w-auto grid-cols-2 gap-0.5 rounded-lg border bg-background p-0.5 group-data-[orientation=horizontal]/tabs:h-auto"
                >
                    <TabsTrigger value="collections" className={TAB_CLASS}>
                        Collections
                        <CountBadge>{collections.length}</CountBadge>
                    </TabsTrigger>
                    <TabsTrigger value="tables" className={TAB_CLASS}>
                        Tables
                        <CountBadge>{tables.length}</CountBadge>
                    </TabsTrigger>
                </TabsList>

                <PlanFilterBar filters={filters} collections={collections} />
            </div>

            <TabsContent value="collections" className="flex min-h-0 flex-col">
                <PlanCollectionsTab filters={filters} />
            </TabsContent>
            <TabsContent value="tables" className="flex min-h-0 flex-col">
                <PlanTablesTab filters={filters} />
            </TabsContent>
        </Tabs>
    );
}
