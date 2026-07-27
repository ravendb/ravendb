import { api } from "@/api/api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { WizardProgress } from "@/components/form/wizard/form-wizard";
import { toWizardStepError } from "@/components/form/wizard/wizard-step-error";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { getTableKey, isTableSupported } from "@/pages/setup/add-app-wizard/discover-utils";
import { discoverTables } from "@/pages/setup/add-app-wizard/steps/verify/use-discover-tables";
import { useFormContext } from "react-hook-form";

export function computeConnectKey(
    connection: Pick<AppFormData["externalConnection"], "provider" | "connectionString" | "slug">,
): string {
    return JSON.stringify({
        provider: connection.provider,
        connectionString: connection.connectionString,
        slug: connection.slug,
    });
}

export function useConnectSourceStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();
    const setDiscoverResult = useSetupWizardStore((state) => state.setDiscoverResult);

    return async (progress: WizardProgress) => {
        const store = useSetupWizardStore.getState();
        const formValues = getValues("externalConnection");
        const connectKey = computeConnectKey(formValues);

        if (connectKey === store.connectKey) {
            return;
        }

        progress.report("Testing connection...");

        const slug = formValues.slug;
        const connectResult = await api.services.setup.connect({
            connectionString: formValues.connectionString,
            provider: formValues.provider,
            slug,
        });

        if (!connectResult.success) {
            throw toWizardStepError(connectResult.errors, "Connection failed.");
        }

        progress.report("Discovering tables...");
        const schemas = store.discoverSchemas;
        const discoverResult = await discoverTables(formValues, schemas, slug);

        setDiscoverResult(discoverResult, schemas);

        // Tables selected under the previous connection may not exist in the new schema; keep
        // only those still verified so the verify step never seeds a stale selection.
        const verifiedKeys = new Set(
            discoverResult.tables
                .filter((table) => isTableSupported(discoverResult, table))
                .map((table) => getTableKey(table)),
        );
        setValue(
            "verifySchema.tables",
            getValues("verifySchema.tables").filter((table) => verifiedKeys.has(getTableKey(table))),
        );

        store.setConnectKey(connectKey);
        store.invalidateMapping();
    };
}
