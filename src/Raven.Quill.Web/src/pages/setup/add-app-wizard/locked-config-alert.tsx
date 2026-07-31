import { LockIcon, PencilIcon } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { ConfirmDialog } from "@/components/shadcn/ui/confirm-dialog";
import { Button } from "@/components/shadcn/ui/button";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";

const LOCKED_COPY = {
    importedConfig: {
        title: "Imported configuration",
        description: "The connection and table selection are locked to match the imported file.",
        confirmDescription:
            "The connection and tables are locked to keep them consistent with the imported table mapping. Changing them after enabling editing does not regenerate the mapping - review it afterwards so it still matches your selection.",
    },
    existingApp: {
        title: "Existing configuration",
        description: "The connection and table selection are locked to the mapping this application already uses.",
        confirmDescription:
            "The connection and tables are locked to keep them consistent with the mapping this application already uses. Changing them after enabling editing does not regenerate the mapping - review it afterwards so it still matches your selection.",
    },
} as const;

export function LockedConfigAlert() {
    const configLock = useSetupWizardStore((state) => state.configLock);
    const isEditingApp = useSetupWizardStore((state) => state.editedAppSlug !== null);
    const unlockConfig = useSetupWizardStore((state) => state.unlockConfig);

    if (configLock === "none") {
        return null;
    }

    if (configLock === "unlocked") {
        return (
            <Alert>
                <PencilIcon />
                <AlertDescription>
                    Editing enabled. The table mapping keeps your edits - review it after changing the connection or
                    table selection.
                </AlertDescription>
            </Alert>
        );
    }

    const copy = isEditingApp ? LOCKED_COPY.existingApp : LOCKED_COPY.importedConfig;

    return (
        <Alert>
            <LockIcon />
            <AlertTitle>{copy.title}</AlertTitle>
            <AlertDescription>
                {copy.description}
                <div className="mt-3">
                    <ConfirmDialog
                        variant="warning"
                        trigger={
                            <Button type="button" variant="outline" size="sm">
                                Enable editing
                            </Button>
                        }
                        title="Enable editing?"
                        description={copy.confirmDescription}
                        confirmLabel="Enable editing"
                        onConfirm={unlockConfig}
                    />
                </div>
            </AlertDescription>
        </Alert>
    );
}
