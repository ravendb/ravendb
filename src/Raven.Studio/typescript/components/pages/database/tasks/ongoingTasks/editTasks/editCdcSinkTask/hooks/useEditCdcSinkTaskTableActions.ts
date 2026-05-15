import useConfirm from "components/common/ConfirmDialog";
import {
    CdcActiveTable,
    editCdcSinkTaskActions,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import {
    EmbeddedTablePath,
    FormEmbeddedTable,
    FormLinkedTable,
    FormRootTable,
    LinkedTablePath,
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
    sourceTableSchema: "public",
    type: "Array",
});

const defaultLinkedTable = (): FormLinkedTable => ({
    joinColumns: [],
    linkedCollectionName: "NewCollection",
    propertyName: "",
    sourceTableName: "",
    sourceTableSchema: "public",
});

export function useEditCdcSinkTaskTableActions() {
    const dispatch = useAppDispatch();
    const confirm = useConfirm();
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

    const changeLinkedToEmbedded = (path: LinkedTablePath) => {
        const { listPath, index } = getTableListLocation(path);
        const linkedTables = getTableList<FormLinkedTable>(listPath);
        const linkedTable = linkedTables[index];
        const embeddedListPath = getSiblingListPath(path, "embeddedTables");
        const embeddedTables = getTableList<FormEmbeddedTable>(embeddedListPath);
        const newPath = castToEmbeddedTablePath(`${embeddedListPath}.${embeddedTables.length}`);

        setFieldValue(
            listPath,
            linkedTables.filter((_, idx) => idx !== index)
        );
        setFieldValue(embeddedListPath, [
            ...embeddedTables,
            {
                ...defaultEmbeddedTable(),
                joinColumns: linkedTable.joinColumns,
                propertyName: linkedTable.propertyName,
                sourceTableName: linkedTable.sourceTableName,
                sourceTableSchema: linkedTable.sourceTableSchema,
            },
        ]);
        dispatch(editCdcSinkTaskActions.tableExpandedRemoved(path));
        dispatch(editCdcSinkTaskActions.activeTableSet({ type: "embedded", path: newPath }));
    };

    const changeEmbeddedToLinked = async (path: EmbeddedTablePath) => {
        const embeddedTable = getValues(path);
        const hasChildren = embeddedTable.embeddedTables?.length > 0 || embeddedTable.linkedTables?.length > 0;

        if (hasChildren) {
            const isConfirmed = await confirm({
                title: "Change to linked table?",
                message: "Nested tables will be removed.",
                icon: "link",
                confirmIcon: "link",
                confirmText: "Change",
                actionColor: "warning",
            });

            if (!isConfirmed) {
                return;
            }
        }

        const { listPath, index } = getTableListLocation(path);
        const embeddedTables = getTableList<FormEmbeddedTable>(listPath);
        const linkedListPath = getSiblingListPath(path, "linkedTables");
        const linkedTables = getTableList<FormLinkedTable>(linkedListPath);
        const newPath = castToLinkedTablePath(`${linkedListPath}.${linkedTables.length}`);

        setFieldValue(
            listPath,
            embeddedTables.filter((_, idx) => idx !== index)
        );
        setFieldValue(linkedListPath, [
            ...linkedTables,
            {
                ...defaultLinkedTable(),
                joinColumns: embeddedTable.joinColumns,
                propertyName: embeddedTable.propertyName,
                sourceTableName: embeddedTable.sourceTableName,
                sourceTableSchema: embeddedTable.sourceTableSchema,
            },
        ]);
        dispatch(editCdcSinkTaskActions.tableExpandedRemoved(path));
        dispatch(editCdcSinkTaskActions.activeTableSet({ type: "linked", path: newPath }));
    };

    return {
        addEmbeddedTable,
        addLinkedTable,
        changeEmbeddedToLinked,
        changeLinkedToEmbedded,
        removeTable,
        toggleRootTableDisabled,
    };
}

function getTableListLocation(path: CdcActiveTable["path"]) {
    const parts = path.split(".");
    const index = Number(parts[parts.length - 1]);
    const listPath = parts.slice(0, -1).join(".") as TableListPath;

    return { index, listPath };
}

function getSiblingListPath(
    path: LinkedTablePath | EmbeddedTablePath,
    siblingListName: "embeddedTables" | "linkedTables"
) {
    const parts = path.split(".");
    const parentPath = parts.slice(0, -2).join(".");

    return `${parentPath}.${siblingListName}` as TableListPath;
}
