import { useFormContext, useWatch } from "react-hook-form";
import { InfoIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

const DRY_RUN_STEPS: Record<AppFormData["externalConnection"]["provider"], string> = {
    Npgsql: "creates a temporary publication and replication slot, reads a single row from each selected table, then drops both",
    SqlClient:
        "enables CDC for the selected tables (and for the database itself, if it was not enabled already), reads a single row from each selected table, then disables exactly what it enabled",
    MySqlConnectorFactory:
        "reads a single row from each selected table through the binary log, without creating anything on your source",
};

export function CdcDryRunAlert() {
    const { control } = useFormContext<AppFormData>();
    const provider = useWatch({ control, name: "externalConnection.provider" });

    return (
        <Alert>
            <InfoIcon />
            <AlertTitle>Continuing runs a CDC dry run against your source database</AlertTitle>
            <AlertDescription>
                Quill {DRY_RUN_STEPS[provider]}. Existing rows are never modified, nothing is dropped, and no data is
                copied to RavenDB yet. The dry run catches blockers the schema scan cannot see, such as a missing CREATE
                or REPLICATION grant. If the cleanup does not finish you get a warning naming what was left behind.
            </AlertDescription>
        </Alert>
    );
}
