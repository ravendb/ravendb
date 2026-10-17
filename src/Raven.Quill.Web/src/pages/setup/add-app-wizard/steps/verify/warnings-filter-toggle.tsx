import { TriangleAlertIcon } from "lucide-react";
import { FilterCount } from "@/components/data/filter-count";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import type { WarningsFilter } from "@/pages/setup/add-app-wizard/discover-utils";

type WarningsFilterToggleProps = {
    value: WarningsFilter;
    onValueChange: (value: WarningsFilter) => void;
    tableCount: number;
    warningTableCount: number;
};

export function WarningsFilterToggle({
    value,
    onValueChange,
    tableCount,
    warningTableCount,
}: WarningsFilterToggleProps) {
    return (
        <ToggleGroup
            type="single"
            variant="outline"
            spacing={0}
            value={value}
            // Radix clears the value when the active item is clicked again; show every table instead.
            onValueChange={(nextValue) => onValueChange((nextValue || "all") as WarningsFilter)}
            aria-label="Filter tables by warnings"
        >
            <ToggleGroupItem value="all">
                All
                <FilterCount value={tableCount} />
            </ToggleGroupItem>
            <ToggleGroupItem value="no-warnings">
                No warnings
                <FilterCount value={tableCount - warningTableCount} />
            </ToggleGroupItem>
            <ToggleGroupItem value="warnings">
                <TriangleAlertIcon className="text-amber-600 dark:text-amber-400" aria-hidden="true" />
                Warnings
                <FilterCount value={warningTableCount} />
            </ToggleGroupItem>
        </ToggleGroup>
    );
}
