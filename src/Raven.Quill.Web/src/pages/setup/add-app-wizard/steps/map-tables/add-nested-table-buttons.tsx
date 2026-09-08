import { Layers, Link2 } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import type { EmbeddedTablePath, RootTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { useTableActions } from "@/pages/setup/add-app-wizard/steps/map-tables/use-table-actions";

export function AddNestedTableButtons({ path }: { path: RootTablePath | EmbeddedTablePath }) {
    const tableActions = useTableActions();

    return (
        <div className="flex flex-wrap gap-2">
            <Button type="button" variant="outline" size="sm" onClick={() => tableActions.addEmbeddedTable(path)}>
                <Layers aria-hidden="true" /> Add embedded table
            </Button>
            <Button type="button" variant="outline" size="sm" onClick={() => tableActions.addLinkedTable(path)}>
                <Link2 aria-hidden="true" /> Add linked table
            </Button>
        </div>
    );
}
