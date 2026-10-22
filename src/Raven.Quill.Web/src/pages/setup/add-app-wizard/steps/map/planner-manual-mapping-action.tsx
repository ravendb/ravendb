import { Table } from "lucide-react";
import type { WizardHeaderComponentProps } from "@/components/form/wizard/form-wizard";
import { Button } from "@/components/shadcn/ui/button";
import { useSwitchToManualMapping } from "@/pages/setup/add-app-wizard/steps/map/use-switch-to-manual-mapping";

/** The way out of the planner, always at hand: a running turn is stopped on the way. */
export function PlannerManualMappingAction({ isBusy, advanceWith }: WizardHeaderComponentProps) {
    const switchToManualMapping = useSwitchToManualMapping();

    return (
        <Button variant="outline" size="lg" disabled={isBusy} onClick={() => advanceWith(switchToManualMapping)}>
            <Table data-icon="inline-start" />
            Use manual mapping
        </Button>
    );
}
