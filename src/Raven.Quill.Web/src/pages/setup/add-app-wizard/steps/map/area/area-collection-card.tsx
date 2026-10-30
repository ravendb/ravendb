import type { ReactNode } from "react";
import { Sparkle, Table2, Trash2, Undo2 } from "lucide-react";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import {
    PlanCollectionSources,
    PlanProblems,
    PlanStatusBadge,
} from "@/pages/setup/add-app-wizard/steps/map/plan/plan-collection-parts";
import { useCanAsk, useCanEditPlan } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import { isMapped, type PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";
import { useToggleRemoval } from "@/pages/setup/add-app-wizard/steps/map/use-toggle-removal";

export function AreaCollectionCard({ collection }: { collection: PlanCollection }) {
    const actions = usePlannerActions();
    const canEdit = useCanEditPlan();
    const canAsk = useCanAsk(collection.area);
    const saveIssues = useSetupWizardStore((state) => state.plannerSaveIssues[collection.name]);
    const toggleRemoval = useToggleRemoval(collection);
    const isRemoving = collection.edit?.isRemoved ?? false;
    const isFailed = collection.state === "failed";
    const canOpen = isMapped(collection);
    const problems = [...collection.problems, ...(saveIssues?.map((issue) => issue.message) ?? [])];

    const summary = (
        <>
            <span className="flex flex-wrap items-center gap-x-2 gap-y-1.5 pr-24">
                <Text as="span" variant="label" className="text-[0.9375rem]">
                    {collection.name}
                </Text>
                <PlanStatusBadge collection={collection} />
                {collection.warnings.length > 0 && (
                    <Badge variant="warning">{formatCount(collection.warnings.length, "warning")}</Badge>
                )}
                {isRemoving && <Badge variant="primary">Removing, unsaved</Badge>}
            </span>
            {!isFailed && collection.why && <Text className="line-clamp-2 text-text-soft">{collection.why}</Text>}
            <PlanCollectionSources collection={collection} />
        </>
    );

    return (
        <article
            className={cn(
                "relative flex flex-col gap-2.5 rounded-lg border bg-card px-4 py-3.5",
                isFailed && "border-badge-destructive-border",
                (isRemoving || collection.state === "removed") && "border-dashed bg-transparent",
            )}
        >
            {canOpen ? (
                <button
                    type="button"
                    className="-m-1 flex flex-col gap-1.5 rounded-md p-1 text-left"
                    onClick={() => actions.openCollection(collection.area, collection.name)}
                >
                    {summary}
                </button>
            ) : (
                <div className="flex flex-col gap-1.5">{summary}</div>
            )}
            {(isFailed || canOpen) && (
                <div className="absolute top-3 right-4 flex gap-1">
                    {isFailed && (
                        <CardAction
                            label="Ask the planner to fix it"
                            disabled={!canAsk}
                            onClick={() => actions.askToFix(collection)}
                        >
                            <Sparkle className="text-primary-strong" aria-hidden="true" />
                        </CardAction>
                    )}
                    {canOpen && (
                        <CardAction
                            label="Edit by hand"
                            disabled={!canEdit}
                            onClick={() => actions.openCollection(collection.area, collection.name)}
                        >
                            <Table2 aria-hidden="true" />
                        </CardAction>
                    )}
                    <CardAction
                        label={isRemoving ? `Keep ${collection.name} in the plan` : "Remove"}
                        disabled={!canEdit}
                        isDestructive={!isRemoving}
                        onClick={toggleRemoval}
                    >
                        {isRemoving ? <Undo2 aria-hidden="true" /> : <Trash2 aria-hidden="true" />}
                    </CardAction>
                </div>
            )}
            {problems.length > 0 && <PlanProblems problems={problems} />}
        </article>
    );
}

function CardAction({
    label,
    disabled,
    isDestructive = false,
    onClick,
    children,
}: {
    label: string;
    disabled: boolean;
    isDestructive?: boolean;
    onClick: () => void;
    children: ReactNode;
}) {
    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <Button
                    variant="outline"
                    size="icon-sm"
                    aria-label={label}
                    disabled={disabled}
                    className={cn(isDestructive && "text-badge-destructive-fg hover:bg-badge-destructive-bg")}
                    onClick={onClick}
                >
                    {children}
                </Button>
            </TooltipTrigger>
            <TooltipContent>{label}</TooltipContent>
        </Tooltip>
    );
}
