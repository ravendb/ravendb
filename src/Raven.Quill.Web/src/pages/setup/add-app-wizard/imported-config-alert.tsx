import { LockIcon, PencilIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { Button } from "@/components/shadcn/ui/button";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";

export function ImportedConfigAlert() {
    const importState = useSetupWizardStore((state) => state.importState);
    const unlockImportedConfig = useSetupWizardStore((state) => state.unlockImportedConfig);

    if (importState === "none") {
        return null;
    }

    if (importState === "unlocked") {
        return (
            <Alert>
                <PencilIcon />
                <AlertDescription>
                    Editing enabled. Changing the connection or table selection will regenerate the table mapping.
                </AlertDescription>
            </Alert>
        );
    }

    return (
        <Alert>
            <LockIcon />
            <AlertTitle>Imported configuration</AlertTitle>
            <AlertDescription>
                The connection and table selection are locked to match the imported file.
                <div className="mt-3">
                    <ConfirmDialog
                        variant="warning"
                        trigger={
                            <Button type="button" variant="outline" size="sm">
                                Enable editing
                            </Button>
                        }
                        title="Enable editing?"
                        description="The connection and tables are locked to keep them consistent with the imported table mapping. If you change them after enabling editing, the mapping is regenerated and any manual edits to it are lost."
                        confirmLabel="Enable editing"
                        onConfirm={unlockImportedConfig}
                    />
                </div>
            </AlertDescription>
        </Alert>
    );
}
