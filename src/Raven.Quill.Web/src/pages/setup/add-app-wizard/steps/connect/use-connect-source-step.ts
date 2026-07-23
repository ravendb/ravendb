import { api } from "@/api/api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { toConnectionError } from "@/pages/setup/add-app-wizard/steps/connect/connect-error";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { discoverTables } from "@/pages/setup/add-app-wizard/steps/verify/use-discover-tables";
import { useFormContext } from "react-hook-form";

export function computeConnectKey(
    connection: Pick<AppFormData["externalConnection"], "provider" | "connectionString">,
): string {
    return JSON.stringify({ provider: connection.provider, connectionString: connection.connectionString });
}

export function useConnectSourceStep() {
    const { getValues } = useFormContext<AppFormData>();
    const setDiscoverResult = useSetupWizardStore((state) => state.setDiscoverResult);

    return async () => {
        const store = useSetupWizardStore.getState();
        const formValues = getValues("externalConnection");
        const connectKey = computeConnectKey(formValues);

        if (connectKey === store.connectKey) {
            return;
        }

        const connectResult = await api.services.setup.connect({
            connectionString: formValues.connectionString,
            provider: formValues.provider,
        });

        if (!connectResult.success) {
            throw toConnectionError(connectResult.errors);
        }

        const schemas = store.discoverSchemas;
        const discoverResult = await discoverTables(formValues, schemas);

        setDiscoverResult(discoverResult, schemas);
        store.setConnectKey(connectKey);
        store.invalidateMapping();
    };
}
