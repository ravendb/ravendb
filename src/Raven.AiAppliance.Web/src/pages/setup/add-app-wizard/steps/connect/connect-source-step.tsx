import { useFormContext } from "react-hook-form";
import { FormInput } from "@/components/form/form-input";
import { FormSelect, type FormSelectOption } from "@/components/form/form-select";
import { FormTextarea } from "@/components/form/form-textarea";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

export function ConnectSourceStep({ isBusy }: WizardBodyComponentProps) {
    const { control } = useFormContext<AppFormData>();

    return (
        <div className="grid gap-5">
            <FormInput
                control={control}
                name="externalConnection.appName"
                label="Application name"
                placeholder="e.g. AcmeShop"
                disabled={isBusy}
            />
            <FormSelect
                control={control}
                name="externalConnection.provider"
                label="Database type"
                options={PROVIDER_OPTIONS}
                disabled={isBusy}
            />
            <FormTextarea
                control={control}
                name="externalConnection.connectionString"
                label="Connection string"
                placeholder="Host=localhost;Port=5432;Database=my_db;Username=admin;Password=pass"
                textareaClassName="font-mono text-xs"
                disabled={isBusy}
            />
        </div>
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
