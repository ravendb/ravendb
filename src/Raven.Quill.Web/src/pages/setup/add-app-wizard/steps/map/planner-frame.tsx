import type { ReactNode } from "react";
import { useWatch } from "react-hook-form";
import { Heading, Text } from "@/components/typography";
import { formatCount } from "@/lib/format";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

export function PlannerFrame({ actions, children }: { actions?: ReactNode; children?: ReactNode }) {
    const tables = useWatch<AppFormData, "verifySchema.tables">({ name: "verifySchema.tables" });
    const catalogName = useSetupWizardStore((state) => state.discoverResult?.catalogName);

    return (
        <div className="flex min-h-80 flex-1 flex-col overflow-hidden rounded-lg border bg-background">
            <div className="flex min-h-12 flex-wrap items-center gap-2 border-b py-1.5 pr-2 pl-4">
                <div className="flex min-w-0 flex-[1_1_15rem] items-baseline gap-2">
                    <Heading as="h2" variant="label">
                        Planner
                    </Heading>
                    <Text variant="muted" as="span" className="truncate">
                        {describeTables(tables, catalogName)}
                    </Text>
                </div>
                {actions}
            </div>
            {children}
        </div>
    );
}

/** "6 tables from demo_shop" when the catalog is known, "34 tables in public" when they share a schema. */
function describeTables(tables: AppFormData["verifySchema"]["tables"], catalogName: string | null | undefined): string {
    const tableCount = formatCount(tables.length, "table");
    const schemas = new Set(tables.map((table) => table.sourceTableSchema));
    const [sharedSchema] = schemas;

    if (catalogName) {
        return `${tableCount} from ${catalogName}`;
    }

    return schemas.size === 1 && sharedSchema ? `${tableCount} in ${sharedSchema}` : tableCount;
}
