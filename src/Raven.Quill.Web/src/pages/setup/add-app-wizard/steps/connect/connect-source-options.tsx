import type { ReactNode } from "react";
import { DEFAULT_PROVIDER, type Provider } from "@/pages/setup/add-app-wizard/connection-string";
import { MySqlIcon, PostgreSqlIcon, SqlServerIcon } from "@/pages/setup/add-app-wizard/steps/connect/provider-icons";

type ProviderOption = {
    value: Provider;
    label: string;
    icon: ReactNode;
};

export const PROVIDER_OPTIONS: ProviderOption[] = [
    {
        value: "Npgsql",
        label: "PostgreSQL",
        icon: <PostgreSqlIcon className="size-5 shrink-0" />,
    },
    {
        value: "SqlClient",
        label: "SQL Server",
        icon: <SqlServerIcon className="size-5 shrink-0" />,
    },
    {
        value: "MySqlConnectorFactory",
        label: "MySQL",
        icon: <MySqlIcon className="size-5 shrink-0" />,
    },
];

/** Maps the display label an existing app reports as its source type (e.g. "PostgreSQL") back to a
 * provider. An unknown source type falls back to the default. */
export function resolveProviderFromSourceType(sourceType: string): Provider {
    return PROVIDER_OPTIONS.find((option) => option.label === sourceType)?.value ?? DEFAULT_PROVIDER;
}
