import { useFormContext } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormTextarea } from "@/components/form/form-textarea";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { StepSection } from "@/pages/setup/add-app-wizard/app-wizard-step-section";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";

export function ConnectSourceStep(props: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();

    return (
        <StepSection {...props}>
            <div className="grid gap-5">
                <FormInput
                    control={control}
                    name="externalConnection.appName"
                    label="Application name"
                    placeholder="e.g. AcmeShop"
                    disabled={props.status === "pending"}
                />
                <FormSelect
                    control={control}
                    name="externalConnection.provider"
                    label="Database type"
                    options={PROVIDER_OPTIONS}
                    disabled={props.status === "pending"}
                />
                <FormTextarea
                    control={control}
                    name="externalConnection.connectionString"
                    label="Connection string"
                    placeholder="Host=localhost;Port=5432;Database=my_db;Username=admin;Password=pass"
                    textareaClassName="font-mono text-xs"
                    disabled={props.status === "pending"}
                />
            </div>
        </StepSection>
    );
}

const PROVIDER_OPTIONS: FormSelectOption<AppFormData["externalConnection"]["provider"]>[] = [
    {
        value: "Npgsql",
        label: "PostgreSQL",
    },
    {
        value: "SqlClient",
        label: "SQL Server",
    },
    {
        value: "MySqlConnectorFactory",
        label: "MySQL",
    },
];
