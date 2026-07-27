import { useFormContext } from "react-hook-form";
import { toast } from "sonner";
import { api } from "@/api/api";
import { toWizardStepError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";

export function computeVerifyCdcKey(verify: {
    connectKey: string | null;
    selectedTables: AppFormData["verifySchema"]["tables"];
}): string {
    return JSON.stringify({
        connectKey: verify.connectKey,
        selectedTables: verify.selectedTables.map(getTableKey).sort(),
    });
}

export function useVerifyCdcStep() {
    const { getValues } = useFormContext<AppFormData>();

    return async () => {
        const store = useSetupWizardStore.getState();
        const selectedTables = getValues("verifySchema.tables");
        const verifyCdcKey = computeVerifyCdcKey({ connectKey: store.connectKey, selectedTables });

        if (verifyCdcKey === store.verifiedCdcKey) {
            return;
        }

        const result = await api.services.setup.verifyCdc({
            tables: selectedTables.map((table) => ({
                sourceTableSchema: table.sourceTableSchema,
                sourceTableName: table.sourceTableName,
            })),
        });

        if (!result.success) {
            throw toWizardStepError(result.errors, "CDC verification failed for the selected tables.");
        }

        if (result.warnings.length > 0) {
            toast.warning("CDC verification passed with warnings", {
                description: result.warnings.join("\n"),
            });
        }

        store.setVerifiedCdcKey(verifyCdcKey);
    };
}
