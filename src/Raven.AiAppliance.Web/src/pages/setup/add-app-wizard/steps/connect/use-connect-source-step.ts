import { api } from "@/api/api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { useMutation } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";

export function useConnectSourceStep() {
    const { getValues } = useFormContext<AppFormData>();
    const setDiscoverResult = useSetupWizardStore((state) => state.setDiscoverResult);

    const connectAndDiscover = useMutation({
        mutationFn: async () => {
            const formValues = getValues("externalConnection");

            const connectResult = await api.services.setup.connect({
                connectionString: formValues.connectionString,
                provider: formValues.provider,
                tableNames: ["users", "orders"], // TODO null
            });

            if (!connectResult.success) {
                throw Error(connectResult.errors?.join("\n") || "Connection failed.");
            }

            const discoverResult = await api.services.setup.discover({
                connectionString: formValues.connectionString,
                provider: formValues.provider,
                tableNames: ["users", "orders"], // TODO null
            });

            setDiscoverResult(discoverResult);

            return true;
        },
    });

    return connectAndDiscover;
}
