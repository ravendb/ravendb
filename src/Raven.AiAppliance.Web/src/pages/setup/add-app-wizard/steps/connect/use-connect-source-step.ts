import { api } from "@/api/api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { discoverTables } from "@/pages/setup/add-app-wizard/steps/verify/use-discover-tables";
import { useFormContext } from "react-hook-form";

export function useConnectSourceStep() {
    const { getValues } = useFormContext<AppFormData>();
    const setDiscoverResult = useSetupWizardStore((state) => state.setDiscoverResult);

    return async () => {
        const formValues = getValues("externalConnection");

        const connectResult = await api.services.setup.connect({
            connectionString: formValues.connectionString,
            provider: formValues.provider,
        });

        if (!connectResult.success) {
            throw Error(connectResult.errors?.join("\n") || "Connection failed.");
        }

        // The first discovery always uses the connection's default schema. Custom
        // schemas can be picked later on the verify step.
        const discoverResult = await discoverTables(formValues, []);

        setDiscoverResult(discoverResult, []);
    };
}
