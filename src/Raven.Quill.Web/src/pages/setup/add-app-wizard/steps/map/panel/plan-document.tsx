import { Brackets, CornerDownRight, Link } from "lucide-react";
import type { CdcSinkRelationType } from "@/api/generated/server-api";
import { Badge } from "@/components/shadcn/ui/badge";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { SectionTable } from "@/components/table/section-table";
import { Text } from "@/components/typography";
import {
    getShapeRows,
    type PlanDocument,
    type PlanProperty,
} from "@/pages/setup/add-app-wizard/steps/map/session/planner-document";

const SHAPE_HEADERS = ["Property", "Comes from"];

const EMBED_SUFFIXES: Record<CdcSinkRelationType, string> = {
    Array: "[ ]",
    Map: "{ }",
    Value: "",
};

export function PlanShapeTable({ document, previous }: { document: PlanDocument; previous: PlanDocument | null }) {
    return (
        <SectionTable headers={SHAPE_HEADERS} isEmpty={document.properties.length === 0} emptyMessage="No properties.">
            {getShapeRows(document, previous).map(({ property, depth, isNew }, index) => (
                <TableRow key={index} className="hover:bg-transparent">
                    <TableCell
                        className="align-top whitespace-normal"
                        style={{ paddingLeft: `${0.5 + depth * 0.875}rem` }}
                    >
                        <span className="inline-flex flex-wrap items-center gap-x-2 gap-y-1.5">
                            {depth > 0 && (
                                <CornerDownRight className="size-3 shrink-0 text-muted-foreground" aria-hidden="true" />
                            )}
                            <Text as="span" variant="code">
                                {formatPropertyName(property)}
                            </Text>
                            {property.kind === "link" && (
                                <Badge variant="outline">
                                    <Link aria-hidden="true" />
                                    Link
                                </Badge>
                            )}
                            {property.kind === "embed" && (
                                <Badge variant="secondary">
                                    <Brackets aria-hidden="true" />
                                    Embed
                                </Badge>
                            )}
                            {isNew && <Badge variant="info">New</Badge>}
                        </span>
                    </TableCell>
                    <TableCell className="align-top whitespace-normal">
                        <PropertySource property={property} />
                    </TableCell>
                </TableRow>
            ))}
        </SectionTable>
    );
}

function PropertySource({ property }: { property: PlanProperty }) {
    switch (property.kind) {
        case "column":
            return (
                <Text as="span" variant="code" className="text-muted-foreground">
                    {property.table}.{property.column}
                </Text>
            );
        case "link":
            return (
                <Text as="span">
                    {property.collection} <JoinColumns columns={property.joinColumns} />
                </Text>
            );
        case "embed":
            return (
                <>
                    <Text as="span" variant="code">
                        {property.document.table}
                    </Text>{" "}
                    <JoinColumns columns={property.joinColumns} />
                </>
            );
    }
}

function JoinColumns({ columns }: { columns: string[] }) {
    if (columns.length === 0) {
        return null;
    }

    return (
        <Text as="span" variant="code" className="text-muted-foreground">
            on {columns.join(", ")}
        </Text>
    );
}

function formatPropertyName(property: PlanProperty): string {
    return property.kind === "embed" ? `${property.name}${EMBED_SUFFIXES[property.relation]}` : property.name;
}
