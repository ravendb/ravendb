import useConfirm from "components/common/ConfirmDialog";
import {
    CdcActiveTable,
    editCdcSinkTaskActions,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import {
    EmbeddedTablePath,
    LinkedTablePath,
    RootTablePath,
    castToEmbeddedTablePath,
    castToLinkedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskFormPaths";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppDispatch } from "components/store";
import { FieldPath, useFormContext } from "react-hook-form";

type RootTable = NonNullable<EditCdcSinkTaskFormData["tables"]>[number];
type EmbeddedTable = NonNullable<RootTable["embeddedTables"]>[number];
type LinkedTable = NonNullable<RootTable["linkedTables"]>[number];
type TableListPath = "tables" | `${string}.embeddedTables` | `${string}.linkedTables`;
type FormPath = FieldPath<EditCdcSinkTaskFormData>;

const defaultEmbeddedTable = (): EmbeddedTable => ({
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

const defaultLinkedTable = (): LinkedTable => ({
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

    const setFieldValue = <TValue>(path: FormPath, value: TValue) => {
        setValue(path, value as never, { shouldDirty: true, shouldTouch: true, shouldValidate: true });
    };

    const getTableList = <TTable>(path: TableListPath) => (getValues(path as FormPath) as TTable[]) ?? [];

    const setTableList = <TTable>(path: TableListPath, value: TTable[]) => {
        setFieldValue(path as FormPath, value);
    };

    const addEmbeddedTable = (parentPath: RootTablePath | EmbeddedTablePath) => {
        const listPath = `${parentPath}.embeddedTables` as TableListPath;
        const embeddedTables = getTableList<EmbeddedTable>(listPath);
        const newPath = castToEmbeddedTablePath(`${listPath}.${embeddedTables.length}`);

        setTableList(listPath, [...embeddedTables, defaultEmbeddedTable()]);
        dispatch(editCdcSinkTaskActions.tableExpandedOneSet({ path: parentPath, isExpanded: true }));
        dispatch(editCdcSinkTaskActions.activeTableSet({ type: "embedded", path: newPath }));
    };

    const addLinkedTable = (parentPath: RootTablePath | EmbeddedTablePath) => {
        const listPath = `${parentPath}.linkedTables` as TableListPath;
        const linkedTables = getTableList<LinkedTable>(listPath);
        const newPath = castToLinkedTablePath(`${listPath}.${linkedTables.length}`);

        setTableList(listPath, [...linkedTables, defaultLinkedTable()]);
        dispatch(editCdcSinkTaskActions.tableExpandedOneSet({ path: parentPath, isExpanded: true }));
        dispatch(editCdcSinkTaskActions.activeTableSet({ type: "linked", path: newPath }));
    };

    const toggleRootTableDisabled = (path: RootTablePath) => {
        const isDisabled = getValues(`${path}.disabled`);
        setFieldValue(`${path}.disabled`, !isDisabled);
    };

    const removeTable = (activeTable: CdcActiveTable) => {
        const { listPath, index } = getTableListLocation(activeTable.path);
        const tables = getTableList(listPath);

        setTableList(
            listPath,
            tables.filter((_, idx) => idx !== index)
        );
        dispatch(editCdcSinkTaskActions.activeTableCleared());
        dispatch(editCdcSinkTaskActions.tableExpandedRemoved(activeTable.path));
    };

    const changeLinkedToEmbedded = (path: LinkedTablePath) => {
        const { listPath, index } = getTableListLocation(path);
        const linkedTables = getTableList<LinkedTable>(listPath);
        const linkedTable = linkedTables[index];
        const embeddedListPath = getSiblingListPath(path, "embeddedTables");
        const embeddedTables = getTableList<EmbeddedTable>(embeddedListPath);
        const newPath = castToEmbeddedTablePath(`${embeddedListPath}.${embeddedTables.length}`);

        setTableList(
            listPath,
            linkedTables.filter((_, idx) => idx !== index)
        );
        setTableList(embeddedListPath, [
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
        const embeddedTable = getValues(path as FormPath) as EmbeddedTable;
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
        const embeddedTables = getTableList<EmbeddedTable>(listPath);
        const linkedListPath = getSiblingListPath(path, "linkedTables");
        const linkedTables = getTableList<LinkedTable>(linkedListPath);
        const newPath = castToLinkedTablePath(`${linkedListPath}.${linkedTables.length}`);

        setTableList(
            listPath,
            embeddedTables.filter((_, idx) => idx !== index)
        );
        setTableList(linkedListPath, [
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
