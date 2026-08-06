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
            // Half-filled details would overwrite the string with a fragment, so it only
            // regenerates once every field is in.
            if (hasCompleteFields(connection.fields)) {
                setValue(
                    "externalConnection.connectionString",
                    buildConnectionString(connection.provider, connection.fields),
                );
            }
        } else if (connection.connectionString.trim() !== "") {
            // A blank string has nothing to read - wiping the details with it would throw away
            // whatever was already typed there.
            const { values, droppedKeywords, hasRecognizedKeywords } = parseConnectionString(
                connection.provider,
                connection.connectionString,
            );

            // Same protection when nothing in the string maps to a detail field: the details
            // keep what was typed, and the warning names what could not be carried over.
            if (hasRecognizedKeywords) {
                setValue("externalConnection.fields", values);
            }
            warnAboutDroppedKeywords(droppedKeywords);
        }

        setValue("externalConnection.mode", mode, { shouldValidate: true });
    };

    return { changeMode };
}

function hasCompleteFields(fields: ExternalConnection["fields"]): boolean {
    return (
        fields.host.trim() !== "" &&
        fields.port != null &&
        fields.database.trim() !== "" &&
        fields.username.trim() !== "" &&
        fields.password.trim() !== ""
    );
}

function warnAboutDroppedKeywords(droppedKeywords: string[]) {
    if (droppedKeywords.length === 0) {
        return;
    }

    toast.warning("Some connection string keywords were dropped", {
        description: `The connection details cannot express ${droppedKeywords.join(", ")}.`,
    });
}
