import { useFormContext } from "react-hook-form";
import { toast } from "sonner";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { buildConnectionString, parseConnectionString } from "@/pages/setup/add-app-wizard/connection-string";

type ExternalConnection = AppFormData["externalConnection"];

export function useConnectionSync() {
    const { getValues, setValue } = useFormContext<AppFormData>();

    const changeMode = (mode: ExternalConnection["mode"]) => {
        const connection = getValues("externalConnection");

        if (mode === "raw") {
            setValue(
                "externalConnection.connectionString",
                buildConnectionString(connection.provider, connection.fields),
            );
        } else {
            const { values, droppedKeywords } = parseConnectionString(connection.connectionString);

            setValue("externalConnection.fields", values);
            warnAboutDroppedKeywords(droppedKeywords);
        }

        setValue("externalConnection.mode", mode, { shouldValidate: true });
    };

    return { changeMode };
}

function warnAboutDroppedKeywords(droppedKeywords: string[]) {
    if (droppedKeywords.length === 0) {
        return;
    }

    toast.warning("Some connection string keywords were dropped", {
        description: `The connection details cannot express ${droppedKeywords.join(", ")}.`,
    });
}
