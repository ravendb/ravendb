import { useRef } from "react";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/shadcn/ui/resizable";
import { TooltipProvider } from "@/components/shadcn/ui/tooltip";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { AreaView } from "@/pages/setup/add-app-wizard/steps/map/area/area-view";
import { PlannerChat } from "@/pages/setup/add-app-wizard/steps/map/chat/planner-chat";
import { CollectionPage } from "@/pages/setup/add-app-wizard/steps/map/collection/collection-page";
import { PlanView } from "@/pages/setup/add-app-wizard/steps/map/plan/plan-view";
import { PlannerWorkspaceContext } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import { isMapped, type Plan } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { usePlan } from "@/pages/setup/add-app-wizard/steps/map/session/use-plan";

export function PlannerWorkspace({ isBusy, onChangeSelection }: { isBusy: boolean; onChangeSelection: () => void }) {
    const plan = usePlan();
    const isChatOpen = useSetupWizardStore((state) => state.plannerIsChatOpen);
    const composerRef = useRef<HTMLTextAreaElement>(null);

    return (
        <PlannerWorkspaceContext.Provider value={{ composerRef, isBusy, changeSelection: onChangeSelection }}>
            <TooltipProvider>
                <div className="flex min-h-80 flex-1 overflow-hidden rounded-lg border bg-background">
                    {isChatOpen ? (
                        <ResizablePanelGroup orientation="horizontal">
                            <ResizablePanel minSize="22rem" className="min-w-0">
                                <PlannerMainView plan={plan} />
                            </ResizablePanel>
                            <ResizableHandle withHandle />
                            <ResizablePanel defaultSize="26rem" minSize="20rem" maxSize="45rem" className="min-w-0">
                                <PlannerChat plan={plan} />
                            </ResizablePanel>
                        </ResizablePanelGroup>
                    ) : (
                        <PlannerMainView plan={plan} />
                    )}
                </div>
            </TooltipProvider>
        </PlannerWorkspaceContext.Provider>
    );
}

/** A viewed area or collection that left the plan falls back to its parent. */
function PlannerMainView({ plan }: { plan: Plan }) {
    const view = useSetupWizardStore((state) => state.plannerView);
    const area = plan.areas.find((candidate) => candidate.name === view.area);
    const collection = area?.collections.find((candidate) => candidate.name === view.collection);

    return (
        <div className="h-full min-h-0 flex-1 overflow-y-auto">
            {collection && isMapped(collection) ? (
                <CollectionPage key={collection.name} collection={collection} />
            ) : area ? (
                <AreaView key={area.name} plan={plan} area={area} />
            ) : (
                <PlanView plan={plan} />
            )}
        </div>
    );
}
