import { useFormContext } from "react-hook-form";
import { toast } from "sonner";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    buildConnectionString,
    getPortAfterProviderChange,
    parseConnectionString,
    type ConnectionValues,
} from "@/pages/setup/add-app-wizard/connection-string";

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

    const changeProvider = (provider: ExternalConnection["provider"]) => {
        const connection = getValues("externalConnection");
        const { values, droppedKeywords } = readActiveEditor(connection);
        const fields: ConnectionValues = {
            ...values,
            port: getPortAfterProviderChange(values.port, connection.provider, provider),
        };

        setValue("externalConnection.fields", fields, { shouldValidate: true });

        if (connection.mode === "raw") {
            setValue("externalConnection.connectionString", buildConnectionString(provider, fields));
            warnAboutDroppedKeywords(droppedKeywords);
        }

        setValue("externalConnection.provider", provider, { shouldDirty: true, shouldValidate: true });
    };

    return { changeMode, changeProvider };
}

function readActiveEditor(connection: ExternalConnection) {
    return connection.mode === "raw"
        ? parseConnectionString(connection.connectionString)
        : { values: connection.fields, droppedKeywords: [] };
}

function warnAboutDroppedKeywords(droppedKeywords: string[]) {
    if (droppedKeywords.length === 0) {
        return;
    }

    toast.warning("Some connection string keywords were dropped", {
        description: `The connection details cannot express ${droppedKeywords.join(", ")}.`,
    });
}
