import type { ReactNode } from "react";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { MySqlIcon, PostgreSqlIcon, SqlServerIcon } from "@/pages/setup/add-app-wizard/steps/connect/provider-icons";

type ProviderOption = {
    value: AppFormData["externalConnection"]["provider"];
    label: string;
    icon: ReactNode;
};

export const PROVIDER_OPTIONS: ProviderOption[] = [
    {
        value: "Npgsql",
        label: "PostgreSQL",
        icon: <PostgreSqlIcon className="size-8" />,
    },
    {
        value: "SqlClient",
        label: "SQL Server",
        icon: <SqlServerIcon className="size-8" />,
    },
    {
        value: "MySqlConnectorFactory",
        label: "MySQL",
        icon: <MySqlIcon className="size-8" />,
    },
];
