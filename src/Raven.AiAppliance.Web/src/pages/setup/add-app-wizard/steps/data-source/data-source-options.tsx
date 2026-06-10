import { Database, DatabaseZap } from "lucide-react";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

type DataSourceOption = {
    value: AppFormData["dataSource"]["source"];
    label: string;
    description: string;
    icon: React.ReactNode;
    isDisabled?: boolean;
};

export const DATA_SOURCE_OPTIONS: DataSourceOption[] = [
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
