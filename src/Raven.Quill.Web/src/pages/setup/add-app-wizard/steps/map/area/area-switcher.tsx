import { useState } from "react";
import { ChevronDown } from "lucide-react";
import { Command, CommandEmpty, CommandInput, CommandItem, CommandList } from "@/components/shadcn/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/shadcn/ui/popover";
import { Text } from "@/components/typography";
import { cn } from "@/lib/utils";
import { countBuilt, type Plan, type PlanArea } from "@/pages/setup/add-app-wizard/steps/map/session/planner-plan";
import { useBuildBlockReason, usePlannerActions } from "@/pages/setup/add-app-wizard/steps/map/use-planner-actions";

/** Picking an area that never started builds it, like Build on its card. */
export function AreaSwitcher({ plan, current }: { plan: Plan; current: PlanArea }) {
    const actions = usePlannerActions();
    const buildBlockReason = useBuildBlockReason();
    const [isOpen, setIsOpen] = useState(false);

    const pick = (area: PlanArea) => {
        setIsOpen(false);

        if (!area.isStarted && area.pendingCollections.length > 0 && !buildBlockReason) {
            actions.buildAndOpenArea(area);
        } else {
            actions.openArea(area.name);
        }
    };

    return (
        <Popover open={isOpen} onOpenChange={setIsOpen}>
            <PopoverTrigger className="inline-flex items-center gap-1 rounded-sm font-medium text-foreground">
                {current.name}
                <ChevronDown className="size-3.5" aria-hidden="true" />
                <span className="sr-only">, switch area</span>
            </PopoverTrigger>
            <PopoverContent align="start" className="w-80 p-0">
                <Command>
                    <CommandInput placeholder="Find an area or a collection" />
                    <CommandList>
                        <CommandEmpty>No area or collection matches.</CommandEmpty>
                        {plan.areas.map((area) => (
                            <CommandItem
                                key={area.name}
                                value={area.name}
                                keywords={area.collections.map((collection) => collection.name)}
                                onSelect={() => pick(area)}
                            >
                                <span
                                    aria-hidden="true"
                                    className={cn("size-2.5 shrink-0 rounded-[2px]", getAreaSquareClass(area))}
                                />
                                <span className={cn("truncate", area.name === current.name && "font-semibold")}>
                                    {area.name}
                                </span>
                                <Text as="span" variant="caption" className="ml-auto tabular-nums">
                                    {countBuilt(area.collections)}/{area.collections.length}
                                </Text>
                            </CommandItem>
                        ))}
                    </CommandList>
                </Command>
            </PopoverContent>
        </Popover>
    );
}

function getAreaSquareClass(area: PlanArea): string {
    switch (area.state) {
        case "failed":
            return "bg-destructive";
        case "building":
            return "animate-pulse bg-primary motion-reduce:animate-none";
        case "built":
            return "bg-success";
        case "partlyBuilt":
            return "bg-[linear-gradient(90deg,var(--color-success)_50%,transparent_50%)] ring-1 ring-success ring-inset";
        case "notStarted":
            return "bg-muted-foreground/30";
    }
}
