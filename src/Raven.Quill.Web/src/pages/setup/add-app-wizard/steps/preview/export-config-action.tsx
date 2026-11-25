import { useFormContext, useWatch } from "react-hook-form";
import { DownloadIcon } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/shadcn/ui/button";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import type { WizardFooterComponentProps } from "@/components/form/wizard/form-wizard";
import { toError } from "@/components/form/wizard/wizard-step-error";
import { buildConfigExport, downloadConfig } from "@/pages/setup/add-app-wizard/config-io";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

type ExportConfigActionProps = WizardFooterComponentProps & {
    /** Brings the form's mapping up to date first, for a step whose mapping is not in the form yet. */
    beforeExport?: () => Promise<void>;
    /** There is a mapping to export even though the form holds none yet. */
    hasMapping?: boolean;
};

/** Sits beside the wizard's completion button: exporting is an alternative to creating the app now. */
export function ExportConfigAction({ isBusy, beforeExport, hasMapping }: ExportConfigActionProps) {
    const { control, getValues } = useFormContext<AppFormData>();

    const dataSource = useWatch({ control, name: "dataSource.source" });
    const tables = useWatch({ control, name: "mapTables.tables" });

    // Only an external source has a connection and a table mapping to export.
    if (dataSource !== "external") {
        return null;
    }

    const exportConfig = async () => {
        try {
            await beforeExport?.();
            downloadConfig(buildConfigExport(getValues()));
        } catch (error) {
            toast.error(toError(error).message || "Could not export the configuration.");
        }
    };

    return (
        <ConfirmDialog
            variant="warning"
            trigger={
                <Button
                    type="button"
                    variant="outline"
                    size="lg"
                    disabled={isBusy || !(hasMapping ?? tables.length > 0)}
                >
                    <DownloadIcon aria-hidden="true" />
                    Export configuration
                </Button>
            }
            title="Export configuration?"
            description="The exported file contains the connection string in plain text, including any username and password it holds. Keep it somewhere safe and avoid sharing it."
            confirmLabel="Export"
            onConfirm={() => void exportConfig()}
        />
    );
}
