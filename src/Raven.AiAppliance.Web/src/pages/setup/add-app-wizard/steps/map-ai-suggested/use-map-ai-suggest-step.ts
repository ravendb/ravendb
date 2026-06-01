import { api } from "@/api/api";
import type { CdcSinkTableConfig } from "@/api/generated/server-api";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useMutation } from "@tanstack/react-query";
import { useFormContext } from "react-hook-form";

export function useMapAiSuggestStep() {
    const { getValues } = useFormContext<AppFormData>();

    const connectAndDiscover = useMutation({
        mutationFn: async () => {
            const formValues = getValues("mapAiSuggest");

            await api.services.setup.map({
                tables: formValues.tables as CdcSinkTableConfig[], // TODO fix type
            });

            return true;
        },
    });

    return connectAndDiscover;
}
