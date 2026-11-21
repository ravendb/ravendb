import type { ReactNode } from "react";
import { MessageCircle, Sparkle, Trash2, TriangleAlert, Undo2 } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Heading, Text } from "@/components/typography";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { CollectionMappingEditor } from "@/pages/setup/add-app-wizard/steps/map/collection/collection-mapping-editor";
import { PlanProblems, PlanSourceList } from "@/pages/setup/add-app-wizard/steps/map/plan/plan-collection-parts";
import { PlannerBreadcrumb } from "@/pages/setup/add-app-wizard/steps/map/planner-breadcrumb";
import {
    EditsDisabledNote,
    PlannerUnsavedChanges,
} from "@/pages/setup/add-app-wizard/steps/map/planner-unsaved-changes";
import {
    useCanAsk,
    useCanEditPlan,
    usePlannerWorkspace,
    useShownChatId,
} from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import { getCollectionSources, type PlanCollection } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";
import { useToggleRemoval } from "@/pages/setup/add-app-wizard/steps/map/use-toggle-removal";

const NO_ISSUES: never[] = [];

export function CollectionPage({ collection }: { collection: PlanCollection }) {
    const actions = usePlannerActions();
    const canEdit = useCanEditPlan();
    const entry = useSetupWizardStore((state) => state.plannerEntries[collection.name]);
    const saveIssues = useSetupWizardStore((state) => state.plannerSaveIssues[collection.name] ?? NO_ISSUES);
    const rejectionIssues = useSetupWizardStore((state) => state.plannerRejectionIssues[collection.name] ?? NO_ISSUES);
    const discardCount = useSetupWizardStore((state) => state.plannerDiscardCount);
    const isFailed = collection.state === "failed";
    const problems = [...collection.problems, ...saveIssues.map((issue) => issue.message)];
    const sources = getCollectionSources(collection);

    return (
        <article className="@container mx-auto flex w-full max-w-280 flex-col gap-4 px-4 py-5 2xl:px-7">
            <PlannerBreadcrumb
                items={[
                    { label: "Plan", onClick: actions.openPlan },
                    { label: collection.area, onClick: () => actions.openArea(collection.area) },
                ]}
            >
                <span className="text-foreground">{collection.name} collection</span>
            </PlannerBreadcrumb>

            <header className="flex flex-wrap items-start gap-x-6 gap-y-3">
                <div className="flex min-w-0 flex-[1_1_20rem] flex-col gap-1">
                    <Heading as="h2" variant="section">
                        {collection.name}
                    </Heading>
                    <Text className="max-w-[72ch] text-text-soft">
                        This is the mapping the {collection.area} agent built for {collection.name}: which of your
                        tables it reads, why it is shaped this way, and how each column becomes a property. Change it
                        below; nothing reaches the plan until you save.
                    </Text>
                </div>
                <CollectionActions collection={collection} />
            </header>

            <EditsDisabledNote />
            <PlannerUnsavedChanges />

            <dl className="flex flex-col divide-y border-y">
                {problems.length > 0 && (
                    <SummaryRow label="Problems">
                        <PlanProblems problems={problems} />
                    </SummaryRow>
                )}
                {sources.table && (
                    <SummaryRow label="From">
                        <PlanSourceList sources={[sources.table, ...sources.embeds]} />
                    </SummaryRow>
                )}
                {sources.links.length > 0 && (
                    <SummaryRow label="Links to">
                        <PlanSourceList sources={sources.links} />
                    </SummaryRow>
                )}
                {!isFailed && collection.why && (
                    <SummaryRow label="Why this shape">
                        <Text className="text-text-soft">{collection.why}</Text>
                    </SummaryRow>
                )}
                {collection.retryErrors.length > 0 && (
                    <SummaryRow label="Retrying">
                        <Text className="text-text-soft">{collection.retryErrors.join(" ")}</Text>
                    </SummaryRow>
                )}
                {collection.warnings.length > 0 && (
                    <SummaryRow label={collection.warnings.length === 1 ? "Warning" : "Warnings"}>
                        <ul className="flex flex-col gap-1">
                            {collection.warnings.map((warning, index) => (
                                <li key={index} className="flex items-start gap-2">
                                    <TriangleAlert
                                        className="mt-0.5 size-3.5 shrink-0 text-badge-warning-fg"
                                        aria-hidden="true"
                                    />
                                    <Text as="span" className="text-text-soft">
                                        {warning}
                                    </Text>
                                </li>
                            ))}
                        </ul>
                    </SummaryRow>
                )}
            </dl>

            {entry?.config && (
                <section aria-labelledby="collection-mapping-title">
                    <CollectionMappingEditor
                        key={`${collection.name}@${entry.version}@${entry.status}@${discardCount}`}
                        title={
                            <Heading as="h3" variant="subsection" id="collection-mapping-title">
                                Mapping
                            </Heading>
                        }
                        collection={collection}
                        builtConfig={entry.config}
                        issues={[...rejectionIssues, ...saveIssues]}
                        isDisabled={!canEdit}
                    />
                </section>
            )}
        </article>
    );
}

function CollectionActions({ collection }: { collection: PlanCollection }) {
    const actions = usePlannerActions();
    const { composerRef } = usePlannerWorkspace();
    const chatId = useShownChatId();
    const canAsk = useCanAsk(chatId);
    const canEdit = useCanEditPlan();
    const addPlannerMention = useSetupWizardStore((state) => state.addPlannerMention);
    const setPlannerChatOpen = useSetupWizardStore((state) => state.setPlannerChatOpen);
    const toggleRemoval = useToggleRemoval(collection);
    const isRemoving = collection.edit?.isRemoved ?? false;

    return (
        <div className="flex flex-wrap gap-2">
            {collection.state === "failed" ? (
                <Button variant="outline" disabled={!canAsk} onClick={() => actions.askToFix(collection)}>
                    <Sparkle className="text-primary-strong" data-icon="inline-start" />
                    Ask the planner to fix it
                </Button>
            ) : (
                <Button
                    variant="outline"
                    onClick={() => {
                        addPlannerMention(chatId, collection.name);
                        setPlannerChatOpen(true);
                        requestAnimationFrame(() => composerRef.current?.focus());
                    }}
                >
                    <MessageCircle data-icon="inline-start" />
                    Ask about it
                </Button>
            )}
            <Button variant="outline" disabled={!canEdit} onClick={toggleRemoval}>
                {isRemoving ? <Undo2 data-icon="inline-start" /> : <Trash2 data-icon="inline-start" />}
                {isRemoving ? "Keep in the plan" : "Remove"}
            </Button>
        </div>
    );
}

function SummaryRow({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div className="grid grid-cols-1 gap-x-4 gap-y-1 py-2.5 @xl:grid-cols-[7rem_minmax(0,1fr)]">
            <dt>
                <Text as="span" variant="muted">
                    {label}
                </Text>
            </dt>
            <dd className="min-w-0">{children}</dd>
        </div>
    );
}
