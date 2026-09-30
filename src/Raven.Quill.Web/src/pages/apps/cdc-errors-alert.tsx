import { useQuery } from "@tanstack/react-query";
import { TriangleAlertIcon } from "lucide-react";
import { api } from "@/api/api";
import { Alert, AlertAction, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { CdcErrorsSheet } from "@/pages/apps/cdc-errors-sheet";

export function CdcErrorsAlert({ slug }: { slug: string }) {
    const errorsQuery = useQuery(api.queries.apps.cdcErrors(slug));
    const errorCount = errorsQuery.data?.length ?? 0;

    if (errorCount === 0) {
        return null;
    }

    return (
        <Alert variant="destructive">
            <TriangleAlertIcon />
            <AlertTitle>Sync errors detected</AlertTitle>
            <AlertDescription>
                {errorCount === 1 ? "1 error was" : `${errorCount} errors were`} reported while syncing data changes.
            </AlertDescription>
            <AlertAction>
                <CdcErrorsSheet
                    slug={slug}
                    trigger={
                        <Button variant="destructive" size="sm">
                            View errors
                        </Button>
                    }
                />
            </AlertAction>
        </Alert>
    );
}
