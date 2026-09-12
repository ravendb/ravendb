import { useFormContext, type FieldPath } from "react-hook-form";
import { toast } from "sonner";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    castToEmbeddedTablePath,
    castToLinkedTablePath,
    castToRootTablePath,
    getRootTablePath,
    type FormEmbeddedTable,
    type FormLinkedTable,
    type FormRootTable,
    type MapActiveTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import { getSourceTableLabel } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";

const CHILD_TABLE_LIST_KEYS = new Set(["embeddedTables", "linkedTables"]);

/** Handles a blocked "Next" on the map tables step: with hundreds of (virtualized) tables the
 * invalid one is usually off-screen, so nothing appears to happen. Selects the first invalid
 * table so the editor shows its errors, scrolls the explorer to it, and toasts the error. */
export function useFocusMapTablesError() {
    const { getValues, getFieldState } = useFormContext<AppFormData>();

    return () => {
        const getError = (path: string) => getFieldState(path as FieldPath<AppFormData>).error as unknown;
        const target = findFirstInvalidTable(getValues("mapTables.tables") ?? [], getError);

        if (!target) {
            // No table to focus, so the error is list-level (e.g. every table disabled). The raw
            // view renders no alert for it, so the toast is the only visible reaction there.
            const listMessage = findFirstErrorMessage(getError("mapTables.tables"));

            if (listMessage) {
                toast.error(listMessage);
            }

            return;
        }

        const label = getSourceTableLabel(getValues(target.path)) || "Unassigned table";
        const message = findFirstErrorMessage(getError(target.path));

        toast.error(message ? `Table "${label}": ${message}` : `Table "${label}" has configuration errors.`);

        const store = useSetupWizardStore.getState();

        if (store.isMapTablesRawView) {
            return;
        }

        // The explorer filters by root table name; clear the filter when it would hide the target.
        const rootTable = getValues(castToRootTablePath(target.path.split(".").slice(0, 3).join(".")));
        const normalizedFilter = store.mapTablesFilter.trim().toLowerCase();

        if (normalizedFilter && !(rootTable?.sourceTableName ?? "").toLowerCase().includes(normalizedFilter)) {
            store.setMapTablesFilter("");
        }

        store.focusMapTable(target);
    };
}

function findFirstInvalidTable(tables: FormRootTable[], getError: (path: string) => unknown): MapActiveTable | null {
    for (let index = 0; index < tables.length; index++) {
        const found = visitTable(tables[index], { type: "root", path: getRootTablePath(index) }, getError);

        if (found) {
            return found;
        }
    }

    return null;
}

function visitTable(
    table: FormRootTable | FormEmbeddedTable | FormLinkedTable,
    tableRef: MapActiveTable,
    getError: (path: string) => unknown,
): MapActiveTable | null {
    const error = getError(tableRef.path);

    if (!error || typeof error !== "object") {
        return null;
    }

    const hasOwnError = Object.keys(error).some((key) => !CHILD_TABLE_LIST_KEYS.has(key));

    if (hasOwnError) {
        return tableRef;
    }

    // Children in explorer order: linked tables render before embedded ones.
    const linkedTables = "linkedTables" in table ? table.linkedTables : [];
    for (let index = 0; index < linkedTables.length; index++) {
        const found = visitTable(
            linkedTables[index],
            { type: "linked", path: castToLinkedTablePath(`${tableRef.path}.linkedTables.${index}`) },
            getError,
        );

        if (found) {
            return found;
        }
    }

    const embeddedTables = "embeddedTables" in table ? table.embeddedTables : [];
    for (let index = 0; index < embeddedTables.length; index++) {
        const found = visitTable(
            embeddedTables[index],
            { type: "embedded", path: castToEmbeddedTablePath(`${tableRef.path}.embeddedTables.${index}`) },
            getError,
        );

        if (found) {
            return found;
        }
    }

    // The error object only mentioned child lists but no child matched; fall back to this table.
    return tableRef;
}

/** Depth-first search for the first message in a react-hook-form error subtree. Skips `ref`,
 * which holds a DOM element. */
function findFirstErrorMessage(error: unknown): string | null {
    if (!error || typeof error !== "object") {
        return null;
    }

    const record = error as Record<string, unknown>;

    if (typeof record.message === "string" && record.message) {
        return record.message;
    }

    for (const [key, value] of Object.entries(record)) {
        if (key === "ref") {
            continue;
        }

        const message = findFirstErrorMessage(value);

        if (message) {
            return message;
        }
    }

    return null;
}
