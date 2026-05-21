import {
    CdcActiveTable,
    editCdcSinkTaskActions,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import {
    EmbeddedTablePath,
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
    RootTablePath,
    castToEmbeddedTablePath,
    castToLinkedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppDispatch } from "components/store";
import { FieldPath, useFormContext, UseFormSetValue } from "react-hook-form";

type TableListPath = "tables" | `tables.${number}.embeddedTables` | `tables.${number}.linkedTables`;
type FormPath = FieldPath<EditCdcSinkTaskFormData>;

const defaultEmbeddedTable = (): FormEmbeddedTable => ({
    caseSensitiveKeys: false,
    columns: [],
    embeddedTables: [],
    joinColumns: [],
    linkedTables: [],
    onDelete: {
        ignoreDeletes: false,
        patch: "",
    },
    patch: "",
    primaryKeyColumns: [],
    propertyName: "",
    sourceTableName: "",
    sourceTableSchema: "",
    type: "Array",
});

const defaultLinkedTable = (): FormLinkedTable => ({
    joinColumns: [],
    linkedCollectionName: "",
    propertyName: "",
    sourceTableName: "",
    sourceTableSchema: "",
});

export function useEditCdcSinkTaskTableActions() {
    const dispatch = useAppDispatch();
    const { getValues, setValue } = useFormContext<EditCdcSinkTaskFormData>();

    const setFieldValue: UseFormSetValue<EditCdcSinkTaskFormData> = (path, value) => {
        setValue(path, value, { shouldDirty: true });
    };

    const getTableList = <TTable>(path: TableListPath) => (getValues(path as FormPath) as TTable[]) ?? [];

    const addEmbeddedTable = (parentPath: RootTablePath | EmbeddedTablePath) => {
        const listPath = `${parentPath}.embeddedTables` as TableListPath;
        const embeddedTables = getTableList<FormEmbeddedTable>(listPath);
        const newPath = castToEmbeddedTablePath(`${listPath}.${embeddedTables.length}`);

        setFieldValue(listPath, [...embeddedTables, defaultEmbeddedTable()]);
        dispatch(editCdcSinkTaskActions.tableExpandedOneSet({ path: parentPath, isExpanded: true }));
        dispatch(editCdcSinkTaskActions.activeTableSet({ type: "embedded", path: newPath }));
    };

    const addLinkedTable = (parentPath: RootTablePath | EmbeddedTablePath) => {
        const listPath = `${parentPath}.linkedTables` as TableListPath;
        const linkedTables = getTableList<FormLinkedTable>(listPath);
        const newPath = castToLinkedTablePath(`${listPath}.${linkedTables.length}`);

        setFieldValue(listPath, [...linkedTables, defaultLinkedTable()]);
        dispatch(editCdcSinkTaskActions.tableExpandedOneSet({ path: parentPath, isExpanded: true }));
        dispatch(editCdcSinkTaskActions.activeTableSet({ type: "linked", path: newPath }));
    };

    const toggleRootTableDisabled = (path: RootTablePath) => {
        const isDisabled = getValues(`${path}.disabled`);
        setFieldValue(`${path}.disabled`, !isDisabled);
    };

    const removeTable = (activeTable: CdcActiveTable) => {
        const { listPath, index } = getTableListLocation(activeTable.path);
        const tables = getTableList<FormRootTable>(listPath);

        setFieldValue(
            listPath,
            tables.filter((_, idx) => idx !== index)
        );
        dispatch(editCdcSinkTaskActions.activeTableCleared());
        dispatch(editCdcSinkTaskActions.tableExpandedRemoved(activeTable.path));
    };

    return {
        addEmbeddedTable,
        addLinkedTable,
        removeTable,
        toggleRootTableDisabled,
    };
}

function getTableListLocation(path: CdcActiveTable["path"]) {
    const parts = path.split(".");
    const index = Number(parts.at(-1));
    const listPath = parts.slice(0, -1).join(".") as TableListPath;

    return { index, listPath };
}
