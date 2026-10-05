import { useState } from "react";
import { Separator } from "@/components/shadcn/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { Text } from "@/components/typography";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlannerCollectionCard } from "@/pages/setup/add-app-wizard/steps/map/planner-collection-card";
import { PlannerProposalView } from "@/pages/setup/add-app-wizard/steps/map/planner-proposal";
import { useRemovePlannerCollection } from "@/pages/setup/add-app-wizard/steps/map/use-planner-session";

type ResultsTab = "proposal" | "collections";

/**
 * What the planner proposed and what it actually registered, side by side as tabs. The view
 * follows the session - the proposal first, the collections once there are any - until the
 * operator picks a tab themselves.
 */
export function PlannerResults() {
    const proposal = useSetupWizardStore((state) => state.plannerProposal);
    const collections = useSetupWizardStore((state) => state.plannerCollections);
    const deselected = useSetupWizardStore((state) => state.plannerDeselected);
    const toggle = useSetupWizardStore((state) => state.togglePlannerCollection);
    const dismiss = useSetupWizardStore((state) => state.removePlannerCollection);
    const isStreaming = useSetupWizardStore((state) => state.isPlannerStreaming);
    const removeCollection = useRemovePlannerCollection();
    const [removing, setRemoving] = useState<string | null>(null);
    const [chosenTab, setChosenTab] = useState<ResultsTab | null>(null);

    const registered = Object.values(collections);

    if (!proposal && registered.length === 0) {
        return (
            <div className="flex h-full items-center justify-center p-6">
                <Text variant="muted">Nothing registered yet.</Text>
            </div>
        );
    }

    const tab = chosenTab ?? (registered.length > 0 ? "collections" : "proposal");

    return (
        <Tabs
            value={tab}
            onValueChange={(next) => setChosenTab(next as ResultsTab)}
            className="flex h-full min-h-0 flex-col gap-0"
        >
            <div className="border-b p-3">
                <TabsList>
                    <TabsTrigger value="proposal" disabled={!proposal}>
                        Proposal
                    </TabsTrigger>
                    <TabsTrigger value="collections">Collections ({registered.length})</TabsTrigger>
                </TabsList>
            </div>

            <TabsContent value="proposal" className="min-h-0 flex-1 overflow-y-auto p-3">
                {proposal && <PlannerProposalView proposal={proposal} collections={collections} />}
            </TabsContent>

            <TabsContent value="collections" className="min-h-0 flex-1 overflow-y-auto p-3">
                {registered.length === 0 ? (
                    <Text variant="muted">
                        Nothing registered yet. Tell the planner which proposed collections to build.
                    </Text>
                ) : (
                    <div className="grid gap-3">
                        {registered.map((collection, index) => (
                            <div key={collection.collection} className="grid gap-3">
                                {index > 0 && <Separator />}
                                <PlannerCollectionCard
                                    collection={collection}
                                    isSelected={!deselected[collection.collection]}
                                    onSelectedChange={(isSelected) => toggle(collection.collection, isSelected)}
                                    onDismiss={() => dismiss(collection.collection)}
                                    onRemove={() => {
                                        setRemoving(collection.collection);
                                        void removeCollection(collection.collection).finally(() => setRemoving(null));
                                    }}
                                    isRemoveDisabled={isStreaming || removing !== null}
                                />
                            </div>
                        ))}
                    </div>
                )}
            </TabsContent>
        </Tabs>
    );
}
