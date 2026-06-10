import { api } from "@/api/api";
import { type AppFormData, tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { useFormContext } from "react-hook-form";

export function useMapSchemaStep() {
    const { getValues, setValue } = useFormContext<AppFormData>();

    return async () => {
        const { source, aiPrompt } = getValues("map");

        if (source !== "ai-suggested") {
            return;
        }

        const result = await api.services.setup.suggestCdc({
            intentPrompt: aiPrompt.trim(),
        });

        if (result.status !== "Success" || !result.configuration) {
            throw new Error(result.rationale.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`);
        }
        const tables = tablesSchema.parse(result.configuration.tables);
        setValue("mapAiSuggest.tables", tables);
    };
}
