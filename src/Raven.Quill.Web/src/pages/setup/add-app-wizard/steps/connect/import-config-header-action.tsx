import type { WizardHeaderComponentProps } from "@/components/form/wizard/form-wizard";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { ImportConfigDialog } from "@/pages/setup/add-app-wizard/steps/connect/import-config-dialog";

/** Importing replaces the whole configuration, so it is offered only while a new app is being created. */
export function ImportConfigHeaderAction({ isBusy }: WizardHeaderComponentProps) {
    const isEditingApp = useSetupWizardStore((state) => state.editedAppSlug !== null);

    if (isEditingApp) {
        return null;
    }

    return <ImportConfigDialog disabled={isBusy} />;
}
