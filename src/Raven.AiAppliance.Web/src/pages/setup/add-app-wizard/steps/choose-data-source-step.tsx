import { Database, DatabaseZap } from "lucide-react";
import { cn } from "@/lib/utils";
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useFormContext, useWatch } from "react-hook-form";

export function ChooseDataSourceStep(props: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();
    const source = useWatch({
        control,
        name: "dataSource.source",
    });

    return (
        <StepSection {...props}>
            <div className="grid gap-3 md:grid-cols-2">
                {DATA_SOURCE_OPTIONS.map((option) => {
                    const isSelected = option.value === source;

                    return (
                        <button
                            key={option.value}
                            type="button"
                            disabled={option.isDisabled}
                            aria-pressed={isSelected}
                            className={cn(
                                "min-h-28 rounded-lg border bg-background p-4 text-left transition-colors",
                                "hover:bg-accent hover:text-accent-foreground",
                                isSelected && "border-foreground bg-accent text-accent-foreground",
                                option.isDisabled && "cursor-not-allowed opacity-55 hover:bg-background",
                            )}
                        >
                            {option.icon}
                            <span className="block text-sm font-semibold">{option.label}</span>
                            <span className="mt-2 block text-xs leading-5 text-muted-foreground">
                                {option.description}
                            </span>
                        </button>
                    );
                })}
            </div>
        </StepSection>
    );
}

type DataSourceOption = {
    value: AppFormData["dataSource"]["source"];
    label: string;
    description: string;
    icon: React.ReactNode;
    isDisabled?: boolean;
};

const DATA_SOURCE_OPTIONS: DataSourceOption[] = [
    {
        value: "external",
        label: "External database",
        description: "Mirror data from PostgreSQL, SQL Server, or MySQL via Change Data Capture.",
        icon: <Database className="mb-5 size-5" />,
    },
    {
        value: "ravendb",
        label: "RavenDB database",
        description: "Connect to an existing database on your RavenDB server.",
        isDisabled: true,
        icon: <DatabaseZap className="mb-5 size-5" />,
    },
];
