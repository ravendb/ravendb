import { Database, DatabaseZap } from "lucide-react";
import type { RadioCardOption } from "@/components/form/form-radio-cards";
import { Badge } from "@/components/shadcn/ui/badge";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

export const DATA_SOURCE_OPTIONS: RadioCardOption<AppFormData["dataSource"]["source"]>[] = [
    {
        value: "external",
        label: "External database",
        description: "Mirror data from PostgreSQL, SQL Server, or MySQL via Change Data Capture.",
        icon: <Database className="size-5" />,
    },
    {
        value: "ravendb",
        label: "RavenDB database",
        description: "Connect to an existing database on your RavenDB server.",
        badge: <Badge variant="secondary">Coming soon</Badge>,
        disabled: true,
        icon: <DatabaseZap className="size-5" />,
    },
];
