import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    castToEmbeddedTablePath,
    castToLinkedTablePath,
    getRootTablePath,
    type EmbeddedTablePath,
    type FormEmbeddedTable,
    type FormRootTable,
    type MapTablePath,
    type RootTablePath,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import {
    createEmptyEmbeddedTable,
    createEmptyLinkedTable,
    createEmptyRootTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useRootTablesFieldArray } from "@/pages/setup/add-app-wizard/steps/map-tables/root-tables-field-array";
import { useFormContext } from "react-hook-form";

type ParentTablePath = RootTablePath | EmbeddedTablePath;
type ParentTable = FormRootTable | FormEmbeddedTable;

/** Follows the segments below the root table, e.g. "embeddedTables.1.embeddedTables.0". Only
 * root and embedded tables can hold children, so every list segment is "embeddedTables". */
function getTableWithinRoot(rootTable: FormRootTable, path: ParentTablePath): ParentTable {
    const nestedIndexes = path
        .split(".")
        .slice(3)
        .filter((_, idx) => idx % 2 === 1)
        .map(Number);

    return nestedIndexes.reduce<ParentTable>((table, index) => table.embeddedTables[index], rootTable);
}

export function useTableActions() {
    const { getValues } = useFormContext<AppFormData>();
    const rootTablesFieldArray = useRootTablesFieldArray();
    const setMapActiveTable = useSetupWizardStore((state) => state.setMapActiveTable);
    const expandMapTable = useSetupWizardStore((state) => state.expandMapTable);
    const removeMapTableUiState = useSetupWizardStore((state) => state.removeMapTableUiState);

    /** Changes below a root table go through the root field array's update: unlike a plain
     * setValue, a field array operation revalidates the tables (the form validates on change),
     * matching how the root-level append/remove already behave. */
    const updateRootTable = <TResult>(path: MapTablePath, mutate: (rootTable: FormRootTable) => TResult): TResult => {
        const rootIndex = Number(path.split(".")[2]);
        const rootTable = structuredClone(getValues(getRootTablePath(rootIndex)));
        const result = mutate(rootTable);

        rootTablesFieldArray.update(rootIndex, rootTable);

        return result;
    };

    const addRootTable = () => {
        const nextIndex = (getValues("mapTables.tables") ?? []).length;

        rootTablesFieldArray.append(createEmptyRootTable(), { shouldFocus: false });
        setMapActiveTable({ type: "root", path: getRootTablePath(nextIndex) });
    };

    const addEmbeddedTable = (parentPath: ParentTablePath) => {
        const newIndex = updateRootTable(parentPath, (rootTable) => {
            const parentTable = getTableWithinRoot(rootTable, parentPath);

            parentTable.embeddedTables.push(createEmptyEmbeddedTable());

            return parentTable.embeddedTables.length - 1;
        });

        expandMapTable(parentPath);
        setMapActiveTable({
            type: "embedded",
            path: castToEmbeddedTablePath(`${parentPath}.embeddedTables.${newIndex}`),
        });
    };

    const addLinkedTable = (parentPath: ParentTablePath) => {
        const newIndex = updateRootTable(parentPath, (rootTable) => {
            const parentTable = getTableWithinRoot(rootTable, parentPath);

            parentTable.linkedTables.push(createEmptyLinkedTable());

            return parentTable.linkedTables.length - 1;
        });

        expandMapTable(parentPath);
        setMapActiveTable({
            type: "linked",
            path: castToLinkedTablePath(`${parentPath}.linkedTables.${newIndex}`),
        });
    };

    const toggleRootTableDisabled = (path: RootTablePath) => {
        updateRootTable(path, (rootTable) => {
            rootTable.disabled = !rootTable.disabled;
        });
    };

    const removeTable = (path: MapTablePath) => {
        const segments = path.split(".");
        const index = Number(segments.at(-1));

        if (segments.length === 3) {
            rootTablesFieldArray.remove(index);
        } else {
            const listName = segments.at(-2) as "embeddedTables" | "linkedTables";
            const parentPath = segments.slice(0, -2).join(".") as ParentTablePath;

            updateRootTable(path, (rootTable) => {
                const parentTable = getTableWithinRoot(rootTable, parentPath);

                if (listName === "embeddedTables") {
                    parentTable.embeddedTables.splice(index, 1);
                } else {
                    parentTable.linkedTables.splice(index, 1);
                }
            });
        }

        removeMapTableUiState(path);
    };

    return {
        addRootTable,
        addEmbeddedTable,
        addLinkedTable,
        toggleRootTableDisabled,
        removeTable,
    };
}
