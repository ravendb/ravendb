import type { ReactNode } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { Badge } from "@/components/shadcn/ui/badge";
import { Alert } from "@/components/shadcn/ui/alert";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import type { AgentFormData } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";
import { useCapabilityWizardStore } from "@/pages/setup/add-capability-wizard/capability-wizard-store";
import { StepSection } from "@/pages/setup/add-app-wizard/app-wizard-step-section";

export function ReviewAgentStep(props: WizardBodyComponentProps) {
    const { control } = useFormContext<AgentFormData>();
    const suggestions = useCapabilityWizardStore((state) => state.suggestions);
    const selectedIndex = useWatch({ control, name: "create.selectedIndex" });
    const systemPrompt = useWatch({ control, name: "create.systemPrompt" });
    const connectionStringName = useWatch({ control, name: "connection.connectionStringName" });
    const config = suggestions[selectedIndex];

    if (!config) {
        return (
            <StepSection {...props}>
                <Alert>Go back and pick an AI-suggested agent first.</Alert>
            </StepSection>
        );
    }

    const queryTools = (config.queries ?? [])
        .map((query) => query.name)
        .filter((name): name is string => Boolean(name));
    const parameters = (config.parameters ?? [])
        .map((parameter) => parameter.name)
        .filter((name): name is string => Boolean(name));

    return (
        <StepSection {...props}>
            <div className="grid gap-5">
                <FormInput control={control} name="review.name" label="Agent name" />

                <div className="grid gap-4 rounded-lg border bg-background p-4">
                    <SummaryRow label="System prompt">
                        <p className="text-sm whitespace-pre-wrap text-muted-foreground">{systemPrompt}</p>
                    </SummaryRow>
                    <SummaryRow label="Connection string">
                        <Badge variant="secondary">{connectionStringName}</Badge>
                    </SummaryRow>
                    <SummaryRow label="Query tools">
                        <ChipList items={queryTools} />
                    </SummaryRow>
                    <SummaryRow label="Parameters">
                        <ChipList items={parameters} />
                    </SummaryRow>
                </div>
            </div>
        </StepSection>
    );
}

function SummaryRow({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div className="grid gap-1.5 sm:grid-cols-[10rem_minmax(0,1fr)] sm:gap-4">
            <span className="text-sm font-medium">{label}</span>
            <div className="min-w-0">{children}</div>
        </div>
    );
}

function ChipList({ items }: { items: string[] }) {
    if (items.length === 0) {
        return <span className="text-sm text-muted-foreground">—</span>;
    }

    return (
        <div className="flex flex-wrap gap-1.5">
            {items.map((item) => (
                <Badge key={item} variant="secondary">
                    {item}
                </Badge>
            ))}
        </div>
    );
}
