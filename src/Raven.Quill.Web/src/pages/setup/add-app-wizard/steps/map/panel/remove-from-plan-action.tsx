import { useState } from "react";
import { Trash } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { joinList } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { usePlannerWorkspace } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import { indexDiscovery } from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";
import {
    buildPlanCollections,
    findCollectionsLinkingTo,
    type PlanCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { selectIsPlanSettled } from "@/pages/setup/add-app-wizard/steps/map/session/planner-state";
import {
    useIsRemovingPlannerCollection,
    useRemovePlannerCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/use-remove-planner-collection";

/**
 * Every removal is confirmed, with a warning when other collections link to the one removed. Suggested
 * and failed collections are removed on the server too, which tells the planner on its next turn, so it
 * does not bring them back unasked.
 */
export function RemoveFromPlanAction({
    collection,
    variant,
    className,
}: {
    collection: PlanCollection;
    variant: "icon" | "label";
    className?: string;
}) {
    const { isBusy } = usePlannerWorkspace();
    const isPlanSettled = useSetupWizardStore(selectIsPlanSettled);
    const removeCollection = useRemovePlannerCollection();
    const isRemoving = useIsRemovingPlannerCollection();
    const [isConfirmOpen, setIsConfirmOpen] = useState(false);
    const [linkingCollections, setLinkingCollections] = useState<string[]>([]);

    const requestRemoval = () => {
        const state = useSetupWizardStore.getState();
        const collections = buildPlanCollections(state, indexDiscovery(state.discoverResult));

        setLinkingCollections(findCollectionsLinkingTo(collections, collection.name));
        setIsConfirmOpen(true);
    };

    const isDisabled = !isPlanSettled || isRemoving || isBusy;
    const label = `Remove ${collection.name} from the plan`;

    return (
        <>
            {variant === "icon" ? (
                <Button
                    variant="ghost"
                    size="icon-sm"
                    className={cn("text-muted-foreground hover:text-destructive", className)}
                    aria-label={label}
                    title={label}
                    disabled={isDisabled}
                    onClick={requestRemoval}
                >
                    <Trash aria-hidden="true" />
                </Button>
            ) : (
                <Button
                    variant="ghost"
                    className={cn("text-destructive hover:text-destructive", className)}
                    disabled={isDisabled}
                    onClick={requestRemoval}
                >
                    <Trash data-icon="inline-start" />
                    Remove from plan
                </Button>
            )}
            <ConfirmDialog
                open={isConfirmOpen}
                onOpenChange={setIsConfirmOpen}
                variant="destructive"
                title={`Remove ${collection.name} from the plan?`}
                description={describeRemoval(collection.name, linkingCollections)}
                confirmLabel="Remove"
                onConfirm={() => removeCollection.mutate(collection.name)}
            />
        </>
    );
}

function describeRemoval(collection: string, linkingCollections: string[]): string {
    if (linkingCollections.length === 0) {
        return `${collection} won't go to the mapping.`;
    }

    const isSingle = linkingCollections.length === 1;

    return (
        `${joinList(linkingCollections)} ${isSingle ? "links" : "link"} to ${collection}. ` +
        `Without ${collection} in the plan, ${isSingle ? "that link points" : "those links point"} to documents that won't exist.`
    );
}
