import type { FormSelectOption } from "@/components/form/form-select";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

export const PROVIDER_OPTIONS: FormSelectOption<AppFormData["externalConnection"]["provider"]>[] = [
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
