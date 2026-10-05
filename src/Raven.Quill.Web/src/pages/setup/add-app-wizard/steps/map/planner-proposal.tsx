import { Badge } from "@/components/shadcn/ui/badge";
import { Separator } from "@/components/shadcn/ui/separator";
import { Heading, Text } from "@/components/typography";
import type { PlannerCollection, PlannerProposal } from "@/pages/setup/add-app-wizard/app-wizard-store";

/**
 * The plan the operator chose from, kept in view after registration starts. Each proposed
 * collection shows where it stands, so the proposal doubles as a checklist of what is still owed.
 */
export function PlannerProposalView({
    proposal,
    collections,
}: {
    proposal: PlannerProposal;
    collections: Record<string, PlannerCollection>;
}) {
    return (
        <div className="grid gap-5">
            {proposal.areas.length > 0 && (
                <section className="grid gap-2">
                    <Heading as="h3" variant="subsection">
                        Areas
                    </Heading>
                    <ul className="grid gap-2">
                        {proposal.areas.map((area, index) => (
                            <li key={`${index}-${area.area}`} className="grid gap-1">
                                <Text variant="label" as="span">
                                    {area.area}
                                </Text>
                                {area.why && <Text variant="muted">{area.why}</Text>}
                                {area.collections.length > 0 && (
                                    <div className="flex flex-wrap gap-1">
                                        {area.collections.map((name) => (
                                            <Badge key={name} variant="secondary">
                                                {name}
                                            </Badge>
                                        ))}
                                    </div>
                                )}
                            </li>
                        ))}
                    </ul>
                </section>
            )}

            {proposal.collections.length > 0 && (
                <section className="grid gap-3">
                    <Heading as="h3" variant="subsection">
                        Proposed collections
                    </Heading>
                    {proposal.collections.map((proposed, index) => (
                        <div key={`${index}-${proposed.collection}`} className="grid gap-2">
                            {index > 0 && <Separator />}
                            <div className="flex items-center gap-2">
                                <Text variant="label" as="span" className="min-w-0 flex-1 truncate">
                                    {proposed.collection}
                                </Text>
                                <RegistrationBadge
                                    collection={proposed.collection ? collections[proposed.collection] : undefined}
                                />
                            </div>
                            {proposed.rootTable && (
                                <Text variant="caption" as="p">
                                    from {proposed.rootTable}
                                </Text>
                            )}
                            {proposed.why && <Text variant="muted">{proposed.why}</Text>}
                            {proposed.absorbs.length > 0 && (
                                <ul className="grid gap-1 pl-3">
                                    {proposed.absorbs.map((absorbed, absorbedIndex) => (
                                        <li
                                            key={`${absorbedIndex}-${absorbed.table}`}
                                            className="flex flex-wrap items-baseline gap-2"
                                        >
                                            {absorbed.how && <Badge variant="outline">{absorbed.how}</Badge>}
                                            <Text variant="caption" as="span">
                                                {absorbed.table}
                                                {absorbed.why ? ` - ${absorbed.why}` : ""}
                                            </Text>
                                        </li>
                                    ))}
                                </ul>
                            )}
                        </div>
                    ))}
                </section>
            )}

            {proposal.dropped.length > 0 && (
                <section className="grid gap-2">
                    <Heading as="h3" variant="subsection">
                        Not migrated
                    </Heading>
                    <ul className="grid gap-1">
                        {proposal.dropped.map((dropped, index) => (
                            <li key={`${index}-${dropped.table}`} className="grid gap-0.5">
                                <Text variant="label" as="span">
                                    {dropped.table}
                                </Text>
                                {dropped.why && <Text variant="muted">{dropped.why}</Text>}
                            </li>
                        ))}
                    </ul>
                </section>
            )}

            {proposal.enables.length > 0 && (
                <section className="grid gap-2">
                    <Heading as="h3" variant="subsection">
                        What this enables
                    </Heading>
                    <ul className="grid list-disc gap-1 pl-5">
                        {proposal.enables.map((enabled) => (
                            <li key={enabled}>
                                <Text variant="muted" as="span">
                                    {enabled}
                                </Text>
                            </li>
                        ))}
                    </ul>
                </section>
            )}
        </div>
    );
}

function RegistrationBadge({ collection }: { collection: PlannerCollection | undefined }) {
    if (!collection) {
        return <Badge variant="outline">not registered</Badge>;
    }

    if (collection.status === "rejected") {
        return <Badge variant="destructive">rejected</Badge>;
    }

    return <Badge variant="success">registered</Badge>;
}
