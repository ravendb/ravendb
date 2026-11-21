import { useState } from "react";
import { ChevronRight, Plus } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { Heading, Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { useCanAsk } from "@/pages/setup/add-app-wizard/steps/map/planner-workspace-context";
import type { PlanLeftOutTable } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { composeAddTablesPrompt } from "@/pages/setup/add-app-wizard/steps/map/session/planner-prompts";
import { usePlannerSession } from "@/pages/setup/add-app-wizard/steps/map/session/use-planner-session";

/** Only the planner turns a table into a collection, so adding one is a request in the plan chat. */
export function PlanLeftOut({ tables, isFiltering }: { tables: PlanLeftOutTable[]; isFiltering: boolean }) {
    const session = usePlannerSession();
    const canAsk = useCanAsk(null);
    const setPlannerChatOpen = useSetupWizardStore((state) => state.setPlannerChatOpen);
    const [isOpenByOperator, setIsOpenByOperator] = useState(false);
    const isOpen = isOpenByOperator || isFiltering;

    if (tables.length === 0) {
        return null;
    }

    return (
        <Collapsible open={isOpen} onOpenChange={setIsOpenByOperator} className="flex flex-col gap-1.5">
            <Heading as="h3" variant="label">
                <CollapsibleTrigger className="flex items-center gap-1.5 px-0.5">
                    <ChevronRight
                        className="size-3.5 text-muted-foreground transition-transform data-[open=true]:rotate-90 motion-reduce:transition-none"
                        data-open={isOpen}
                        aria-hidden="true"
                    />
                    Not in the plan
                    <Text as="span" variant="caption" className="font-normal">
                        {formatCount(tables.length, "table")}
                    </Text>
                </CollapsibleTrigger>
            </Heading>
            <CollapsibleContent>
                <ul className="flex flex-col rounded-lg border bg-card">
                    {tables.map((table) => (
                        <li
                            key={table.table}
                            className="flex flex-wrap items-center gap-x-3 gap-y-2 border-b px-3.5 py-3 last:border-b-0"
                        >
                            <div className="flex min-w-0 flex-[1_1_15rem] flex-col gap-0.5">
                                <Text as="span" variant="code">
                                    {table.table}
                                </Text>
                                <Text as="span" variant="caption">
                                    {table.why}
                                </Text>
                            </div>
                            <Button
                                variant="outline"
                                size="sm"
                                disabled={!canAsk}
                                onClick={() => {
                                    setPlannerChatOpen(true);
                                    void session.askProposal({
                                        text: composeAddTablesPrompt([table.table]),
                                        mentions: [],
                                    });
                                }}
                            >
                                <Plus data-icon="inline-start" />
                                Add to plan<span className="sr-only">, {table.table}</span>
                            </Button>
                        </li>
                    ))}
                </ul>
            </CollapsibleContent>
        </Collapsible>
    );
}
