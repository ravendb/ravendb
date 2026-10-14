import { useId, useState } from "react";
import { Check, Search } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/shadcn/ui/input-group";
import { Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableLabel } from "@/pages/setup/add-app-wizard/discover-utils";
import { PlannerVirtualList } from "@/pages/setup/add-app-wizard/steps/map/planner-virtual-list";
import { createNameMatcher, toSearchKey } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";

/** Up to this many tables show as chips; past it the step stays two rows tall and the rest fold away. */
const MAX_CHIPS = 8;

const NAME_ROW_HEIGHT_PX = 24;

export function PlannerSelectedTables({
    tables,
    onChangeSelection,
}: {
    tables: AppFormData["verifySchema"]["tables"];
    onChangeSelection: () => void;
}) {
    const labelId = useId();
    const [isExpanded, setIsExpanded] = useState(false);
    const [filter, setFilter] = useState("");

    const schemas = new Set(tables.map((table) => table.sourceTableSchema ?? ""));
    const sharedSchema = schemas.size === 1 ? tables[0]?.sourceTableSchema : null;
    const names = tables.map((table) => (sharedSchema ? table.sourceTableName : getTableLabel(table)));
    const chips = names.length > MAX_CHIPS ? names.slice(0, MAX_CHIPS - 1) : names;
    const matchesName = createNameMatcher(filter);
    const matchingNames = names.filter((name) => matchesName(toSearchKey(name)));

    return (
        <>
            <span className="flex flex-wrap items-center gap-2">
                <Check className="size-3 shrink-0 text-success" aria-hidden="true" />
                <Text variant="muted" as="span" id={labelId}>
                    Read the schema of {formatCount(tables.length, "table")}
                    {sharedSchema && (
                        <>
                            {" "}
                            in{" "}
                            <Text as="span" variant="code">
                                {sharedSchema}
                            </Text>
                        </>
                    )}
                </Text>
                <Button variant="link" className="h-auto p-0" onClick={onChangeSelection}>
                    Change selection
                </Button>
            </span>

            {isExpanded ? (
                <div className="ml-5 flex flex-col gap-2 rounded-lg border bg-background p-2.5">
                    <div className="flex items-center gap-2">
                        <InputGroup className="flex-1">
                            <InputGroupAddon>
                                <Search aria-hidden="true" />
                            </InputGroupAddon>
                            <InputGroupInput
                                aria-label="Filter tables"
                                placeholder={`Filter ${formatCount(tables.length, "table")}`}
                                value={filter}
                                onChange={(event) => setFilter(event.target.value)}
                            />
                        </InputGroup>
                        <Button
                            variant="ghost"
                            aria-expanded="true"
                            onClick={() => {
                                setIsExpanded(false);
                                setFilter("");
                            }}
                        >
                            Show less
                        </Button>
                    </div>
                    {matchingNames.length === 0 ? (
                        <Text variant="muted" className="px-1.5 py-1">
                            No tables match.
                        </Text>
                    ) : (
                        <div role="list" aria-labelledby={labelId} className="flex max-h-42 flex-col">
                            <PlannerVirtualList
                                items={matchingNames}
                                getKey={(name) => name}
                                estimateSize={() => NAME_ROW_HEIGHT_PX}
                                renderItem={(name) => (
                                    <Text as="div" role="listitem" variant="code" className="truncate px-1.5 py-1">
                                        {name}
                                    </Text>
                                )}
                            />
                        </div>
                    )}
                </div>
            ) : (
                <ul aria-labelledby={labelId} className="flex flex-wrap gap-1.5 pl-5">
                    {chips.map((name) => (
                        <li key={name} className="rounded-sm border bg-background px-2 py-0.5">
                            <Text as="span" variant="code">
                                {name}
                            </Text>
                        </li>
                    ))}
                    {chips.length < names.length && (
                        <li className="flex">
                            <Button
                                variant="outline"
                                size="xs"
                                aria-expanded="false"
                                className="border-dashed"
                                onClick={() => setIsExpanded(true)}
                            >
                                +{names.length - chips.length} more
                            </Button>
                        </li>
                    )}
                </ul>
            )}
        </>
    );
}
