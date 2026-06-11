import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    castToEmbeddedTablePath,
    castToLinkedTablePath,
    getRootTablePath,
    type EmbeddedTablePath,
    type FormEmbeddedTable,
    type FormLinkedTable,
    type FormRootTable,
    type MapTablePath,
    type RootTablePath,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import {
    createEmptyEmbeddedTable,
    createEmptyLinkedTable,
    createEmptyRootTable,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useFormContext, type FieldPath, type PathValue } from "react-hook-form";

type ParentTablePath = RootTablePath | EmbeddedTablePath;
type FormPath = FieldPath<AppFormData>;

export function useTableActions() {
    const { getValues, setValue } = useFormContext<AppFormData>();
    const setMapActiveTable = useSetupWizardStore((state) => state.setMapActiveTable);
    const expandMapTable = useSetupWizardStore((state) => state.expandMapTable);
    const removeMapTableUiState = useSetupWizardStore((state) => state.removeMapTableUiState);

    const getTableList = <TTable>(listPath: string) => (getValues(listPath as FormPath) as TTable[]) ?? [];

    const setFieldValue = (path: string, value: unknown) => {
        setValue(path as FormPath, value as PathValue<AppFormData, FormPath>, { shouldDirty: true });
    };

    const addRootTable = () => {
        const tables = getTableList<FormRootTable>("mapTables.tables");

        setFieldValue("mapTables.tables", [...tables, createEmptyRootTable()]);
        setMapActiveTable({ type: "root", path: getRootTablePath(tables.length) });
    };

    const addEmbeddedTable = (parentPath: ParentTablePath) => {
        const listPath = `${parentPath}.embeddedTables`;
        const embeddedTables = getTableList<FormEmbeddedTable>(listPath);

        setFieldValue(listPath, [...embeddedTables, createEmptyEmbeddedTable()]);
        expandMapTable(parentPath);
        setMapActiveTable({ type: "embedded", path: castToEmbeddedTablePath(`${listPath}.${embeddedTables.length}`) });
    };

    const addLinkedTable = (parentPath: ParentTablePath) => {
        const listPath = `${parentPath}.linkedTables`;
        const linkedTables = getTableList<FormLinkedTable>(listPath);

        setFieldValue(listPath, [...linkedTables, createEmptyLinkedTable()]);
        expandMapTable(parentPath);
        setMapActiveTable({ type: "linked", path: castToLinkedTablePath(`${listPath}.${linkedTables.length}`) });
    };

    const toggleRootTableDisabled = (path: RootTablePath) => {
        setFieldValue(`${path}.disabled`, !getValues(`${path}.disabled`));
    };

    const removeTable = (path: MapTablePath) => {
        const parts = path.split(".");
        const index = Number(parts.at(-1));
        const listPath = parts.slice(0, -1).join(".");
        const tables = getTableList<unknown>(listPath);

        setFieldValue(
            listPath,
            tables.filter((_, idx) => idx !== index),
        );
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
