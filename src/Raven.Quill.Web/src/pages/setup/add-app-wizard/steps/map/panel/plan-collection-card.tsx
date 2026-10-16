import { Fragment, type ReactNode } from "react";
import { ChevronRight, CircleX, Sparkle, TriangleAlert } from "lucide-react";
import { Alert, AlertDescription } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Heading, Text } from "@/components/typography";
import { interleaveListSeparators, joinList, upperFirst } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { PlanStatusBadge } from "@/pages/setup/add-app-wizard/steps/map/panel/plan-status-badge";
import { RemoveFromPlanAction } from "@/pages/setup/add-app-wizard/steps/map/panel/remove-from-plan-action";
import { useCanAskPlanner } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import {
    getMemberTables,
    isBuilt,
    type PlanCollection,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { composeFixCollectionPrompt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

export function PlanCollectionCard({ collection, headingAs }: { collection: PlanCollection; headingAs: "h3" | "h4" }) {
    const openPlannerPanel = useSetupWizardStore((state) => state.openPlannerPanel);
    const isFailed = collection.state === "failed";
    const hasErrors = collection.errors.length > 0;

    return (
        <article
            className={cn(
                "flex flex-col rounded-lg border bg-card py-3 pr-2 pl-3.5",
                isFailed ? "gap-1.5" : "gap-2",
                collection.state === "updated" && "border-badge-info-border",
                hasErrors && "border-badge-destructive-border",
            )}
        >
            <div className={cn("flex items-center", isFailed ? "gap-2.5" : "gap-2")}>
                {isFailed && <CircleX className="size-4 shrink-0 text-destructive" aria-hidden="true" />}
                <Heading as={headingAs} variant="label" className="min-w-0 truncate">
                    {collection.name}
                </Heading>
                <PlanStatusBadge state={collection.state} className="ml-auto" />
                <RemoveFromPlanAction collection={collection} variant="icon" />
                <Button
                    variant="ghost"
                    size="icon-sm"
                    className="text-muted-foreground"
                    aria-label={`Open ${collection.name}`}
                    data-opens-collection={collection.name}
                    onClick={() => openPlannerPanel(collection.name)}
                >
                    <ChevronRight aria-hidden="true" />
                </Button>
            </div>

            {isFailed ? (
                <PlanCollectionFailure collection={collection} className="ml-6.5" />
            ) : (
                <>
                    {collection.why && <Text className="line-clamp-2 pr-1.5 text-text-soft">{collection.why}</Text>}
                    <PlanCollectionSources collection={collection} className="pr-1.5" />
                    {hasErrors && <PlanCollectionFailure collection={collection} />}
                    {collection.warnings.map((warning, index) => (
                        <PlanWarning key={index} className="mt-1 mr-1.5">
                            {warning}
                        </PlanWarning>
                    ))}
                </>
            )}
        </article>
    );
}

export function PlanCollectionFailure({ collection, className }: { collection: PlanCollection; className?: string }) {
    const session = usePlannerSession();
    const canAsk = useCanAskPlanner();

    return (
        <>
            {isBuilt(collection) && (
                <Text variant="label" className={className}>
                    The latest change was rejected. The built version stays as it was.
                </Text>
            )}
            <ul className={cn("mr-1.5 flex list-disc flex-col gap-1 pl-4", className)}>
                {collection.errors.map((error, index) => (
                    <li key={index}>
                        <Text as="span" className="text-text-soft">
                            {error}
                        </Text>
                    </li>
                ))}
            </ul>
            <div className={cn("pt-1", className)}>
                <Button
                    variant="outline"
                    size="sm"
                    disabled={!canAsk}
                    onClick={() =>
                        void session.ask({ text: composeFixCollectionPrompt(collection.name), mentions: [] })
                    }
                >
                    <Sparkle className="text-primary-strong" data-icon="inline-start" />
                    Ask the planner to fix it
                </Button>
            </div>
        </>
    );
}

export function PlanWarning({ className, children }: { className?: string; children: ReactNode }) {
    return (
        <Alert variant="warning" role="note" className={className}>
            <TriangleAlert aria-hidden="true" />
            <AlertDescription>{children}</AlertDescription>
        </Alert>
    );
}

/**
 * Where a collection's documents come from. A suggestion is still a list of tables; a built collection
 * says what it embeds and links, under the properties the planner chose.
 */
function PlanCollectionSources({ collection, className }: { collection: PlanCollection; className: string }) {
    const { document } = collection;
    const links = document?.properties.filter((property) => property.kind === "link") ?? [];

    if (collection.state === "suggested") {
        const tables = [...new Set(getMemberTables(collection))];

        if (tables.length === 0) {
            return null;
        }

        return (
            <Text variant="caption" className={className}>
                From {interleaveListSeparators(tables.map((table) => <Code key={table}>{table}</Code>))}
                {links.length > 0 && `, linked to ${joinList(links.map((link) => link.collection))}`}
            </Text>
        );
    }

    if (!document) {
        return null;
    }

    const embeds = document.properties.filter((property) => property.kind === "embed");
    const parts = [
        ...embeds.map((embed) => ({
            key: `embed-${embed.name}`,
            verb: "embeds",
            target: (
                <>
                    <Code>{embed.document.table}</Code> as <Code>{embed.name}</Code>
                </>
            ),
        })),
        ...links.map((link) => ({
            key: `link-${link.name}`,
            verb: "links",
            target: (
                <>
                    {link.collection} as <Code>{link.name}</Code>
                </>
            ),
        })),
    ];

    if (parts.length === 0) {
        return (
            <Text variant="muted" className={className}>
                From <Code>{document.table}</Code>
            </Text>
        );
    }

    return (
        <Text variant="muted" className={className}>
            {parts.map((part, index) => (
                <Fragment key={part.key}>
                    {index > 0 && " · "}
                    {index === 0 ? upperFirst(part.verb) : part.verb} {part.target}
                </Fragment>
            ))}
        </Text>
    );
}

function Code({ children }: { children: ReactNode }) {
    return (
        <Text as="span" variant="code">
            {children}
        </Text>
    );
}
