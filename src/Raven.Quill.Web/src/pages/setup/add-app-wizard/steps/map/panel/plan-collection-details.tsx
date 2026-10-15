import { useId, useState } from "react";
import { Braces, ChevronLeft, MessageCircle, X } from "lucide-react";
import { CopyableCode } from "@/components/data/copyable-code";
import { Button } from "@/components/shadcn/ui/button";
import { Heading, Text } from "@/components/typography";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlanCollectionFailure, PlanWarning } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-collection-card";
import { PlanShapeTable } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-document";
import { PlanStatusBadge } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-status-badge";
import { RemoveFromPlanAction } from "@/pages/setup/add-app-wizard/steps/map/panel/remove-from-plan-action";
import {
    useCanComposeForPlanner,
    useFocusPlannerComposer,
} from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

export function PlanCollectionDetails({ collection }: { collection: PlanCollection }) {
    const shapeTitleId = useId();
    const focusComposer = useFocusPlannerComposer();
    const canCompose = useCanComposeForPlanner();
    const isMentioned = useSetupWizardStore((state) => state.plannerMentions.includes(collection.name));
    const openPlannerPanel = useSetupWizardStore((state) => state.openPlannerPanel);
    const closePlannerPanel = useSetupWizardStore((state) => state.closePlannerPanel);
    const addPlannerMention = useSetupWizardStore((state) => state.addPlannerMention);
    const removePlannerMention = useSetupWizardStore((state) => state.removePlannerMention);
    const [isJsonShown, setIsJsonShown] = useState(false);

    const toggleMention = () => {
        if (isMentioned) {
            removePlannerMention(collection.name);
            return;
        }

        addPlannerMention(collection.name);
        focusComposer();
    };

    return (
        <>
            <div className="flex items-center gap-1.5 border-b px-2 py-2.5">
                <Button
                    variant="ghost"
                    size="icon-sm"
                    className="text-muted-foreground"
                    aria-label="Back to the plan"
                    autoFocus
                    onClick={() => openPlannerPanel()}
                >
                    <ChevronLeft aria-hidden="true" />
                </Button>
                <div className="flex min-w-0 flex-1 flex-col">
                    <Heading as="h3" variant="subsection" className="truncate">
                        <Text as="span" variant="muted">
                            Plan /
                        </Text>{" "}
                        {collection.name}
                    </Heading>
                    {collection.rootTable && (
                        <Text variant="muted" as="span" className="truncate">
                            From{" "}
                            <Text as="span" variant="code">
                                {collection.rootTable}
                            </Text>
                            {collection.area && `, in ${collection.area}`}
                        </Text>
                    )}
                </div>
                <PlanStatusBadge state={collection.state} />
                <Button
                    variant="ghost"
                    size="icon-sm"
                    className="text-muted-foreground"
                    aria-label="Close plan"
                    onClick={() => {
                        closePlannerPanel();
                        focusComposer();
                    }}
                >
                    <X aria-hidden="true" />
                </Button>
            </div>

            <div className="flex min-h-0 flex-1 flex-col gap-4.5 overflow-y-auto p-4">
                {collection.state !== "failed" && collection.why && (
                    <Text className="text-text-soft">{collection.why}</Text>
                )}

                {collection.errors.length > 0 && (
                    <div className="flex flex-col gap-1.5">
                        <PlanCollectionFailure collection={collection} />
                    </div>
                )}

                {collection.document && (
                    <section aria-labelledby={shapeTitleId} className="flex flex-col gap-2">
                        <Heading as="h4" variant="label" id={shapeTitleId} className="text-muted-foreground">
                            Document shape
                        </Heading>
                        <PlanShapeTable document={collection.document} previous={collection.previousDocument} />
                    </section>
                )}

                {collection.warnings.map((warning, index) => (
                    <PlanWarning key={index}>{warning}</PlanWarning>
                ))}

                <div className="flex flex-wrap items-center gap-1.5 border-t pt-3">
                    <Button
                        variant="outline"
                        aria-pressed={isMentioned}
                        className={cn(isMentioned && "bg-muted dark:bg-muted")}
                        disabled={!canCompose}
                        onClick={toggleMention}
                    >
                        <MessageCircle data-icon="inline-start" />
                        Ask about {collection.name}
                    </Button>
                    {collection.config && (
                        <Button variant="ghost" aria-pressed={isJsonShown} onClick={() => setIsJsonShown(!isJsonShown)}>
                            <Braces data-icon="inline-start" />
                            View JSON
                        </Button>
                    )}
                    <RemoveFromPlanAction collection={collection} variant="label" className="ml-auto" />
                </div>

                {isJsonShown && collection.config && (
                    <CopyableCode
                        code={JSON.stringify(collection.config, null, 2)}
                        copyLabel={`Copy the ${collection.name} mapping`}
                        language="javascript"
                    />
                )}
            </div>
        </>
    );
}
