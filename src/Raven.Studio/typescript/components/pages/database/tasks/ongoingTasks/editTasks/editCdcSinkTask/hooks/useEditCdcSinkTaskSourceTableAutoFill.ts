import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import {
    EmbeddedTablePath,
    LinkedTablePath,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import {
    CdcSinkSourceForeignKey,
    CdcSinkSourceTable,
    CdcSinkSourceTableOption,
    findSourceTable,
    getForeignKeysToTable,
    getSourceSchemaOptions,
    getSourceTableOptions,
    mapSourceColumnsToFormData,
    pascalCase,
    propertyNameFromJoinColumn,
    stringValues,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskSchemaUtils";
import { useAppSelector } from "components/store";
import { useMemo } from "react";
import { FieldPath, PathValue, useFormContext, useWatch } from "react-hook-form";
import { ActionMeta, OnChangeValue } from "react-select";

type AutoFillMode = "embedded" | "linked";
type AutoFillPath = EmbeddedTablePath | LinkedTablePath;
type FormPath = FieldPath<EditCdcSinkTaskFormData>;

export function useEditCdcSinkTaskSourceTableAutoFill(path: AutoFillPath, mode: AutoFillMode) {
    const sourceSchema = useAppSelector(editCdcSinkTaskSelectors.sourceSchema);
    const { control, getValues, setValue } = useFormContext<EditCdcSinkTaskFormData>();
    const parentPath = getParentPath(path);

    const selectedSourceTableSchema = useWatch({
        control,
        name: formPath(`${path}.sourceTableSchema`),
    });
    const parentSourceTableSchema = useWatch({
        control,
        name: formPath(`${parentPath}.sourceTableSchema`),
    });
    const parentSourceTableName = useWatch({
        control,
        name: formPath(`${parentPath}.sourceTableName`),
    });
    const sourceTableSchema = String(selectedSourceTableSchema ?? "");
    const excludedTable = {
        sourceTableSchema: String(parentSourceTableSchema ?? ""),
        sourceTableName: String(parentSourceTableName ?? ""),
    };

    const sourceSchemaOptions = useMemo(() => getSourceSchemaOptions(sourceSchema), [sourceSchema]);
    const sourceTableOptions = useMemo(
        () => getSourceTableOptions(sourceSchema, sourceTableSchema, excludedTable),
        [sourceSchema, sourceTableSchema, excludedTable.sourceTableSchema, excludedTable.sourceTableName]
    );

    const handleSourceTableChange = (
        option: OnChangeValue<CdcSinkSourceTableOption, false>,
        actionMeta: ActionMeta<CdcSinkSourceTableOption>
    ) => {
        if (actionMeta.action !== "select-option" || !option?.table) {
            return;
        }

        const selectedTable = option.table;

        setFieldValue(`${path}.sourceTableName`, selectedTable.SourceTableName);
        setStringIfEmpty(`${path}.sourceTableSchema`, selectedTable.SourceTableSchema);

        if (mode === "embedded") {
            fillEmbeddedTable(selectedTable);
        } else {
            fillLinkedTable(selectedTable);
        }
    };

    const fillEmbeddedTable = (selectedTable: CdcSinkSourceTable) => {
        const relation = getSingleRelation(selectedTable);

        setStringIfEmpty(`${path}.propertyName`, getEmbeddedPropertyName(selectedTable, relation));
        setListIfEmpty(`${path}.primaryKeyColumns`, stringValues(selectedTable.PrimaryKeyColumns));
        setListIfEmpty(`${path}.columns`, mapSourceColumnsToFormData(selectedTable));

        if (relation) {
            setListIfEmpty(`${path}.joinColumns`, stringValues(relation.foreignKey.Columns));
        }
    };

    const fillLinkedTable = (selectedTable: CdcSinkSourceTable) => {
        const relation = getSingleParentToSelectedRelation(selectedTable);

        setStringIfEmpty(`${path}.linkedCollectionName`, pascalCase(selectedTable.SourceTableName));

        if (relation) {
            setStringIfEmpty(`${path}.propertyName`, relation.Columns.map(propertyNameFromJoinColumn).join("And"));
            setListIfEmpty(`${path}.joinColumns`, stringValues(relation.Columns));
        }
    };

    const getSingleRelation = (selectedTable: CdcSinkSourceTable) => {
        const parentTable = getParentSourceTable();

        if (!parentTable) {
            return null;
        }

        const selectedToParent = getForeignKeysToTable(selectedTable, parentTable).map((foreignKey) => ({
            foreignKey,
            type: "selectedToParent" as const,
        }));
        const parentToSelected = getForeignKeysToTable(parentTable, selectedTable).map((foreignKey) => ({
            foreignKey,
            type: "parentToSelected" as const,
        }));
        const relations = [...selectedToParent, ...parentToSelected];

        return relations.length === 1 ? relations[0] : null;
    };

    const getSingleParentToSelectedRelation = (selectedTable: CdcSinkSourceTable) => {
        const parentTable = getParentSourceTable();

        if (!parentTable) {
            return null;
        }

        const relations = getForeignKeysToTable(parentTable, selectedTable);

        return relations.length === 1 ? relations[0] : null;
    };

    const getParentSourceTable = () => {
        const parentTable = getValues(formPath(parentPath)) as {
            sourceTableSchema: string;
            sourceTableName: string;
        };

        return findSourceTable(sourceSchema, parentTable?.sourceTableSchema, parentTable?.sourceTableName);
    };

    const setStringIfEmpty = (fieldPath: string, value: string) => {
        const currentValue = getValues(formPath(fieldPath));

        if (!String(currentValue ?? "").trim()) {
            setFieldValue(fieldPath, value);
        }
    };

    const setListIfEmpty = <T>(fieldPath: string, value: T[]) => {
        const currentValue = getValues(formPath(fieldPath)) as T[];

        if (!currentValue?.length) {
            setFieldValue(fieldPath, value);
        }
    };

    const setFieldValue = (fieldPath: string, value: unknown) => {
        setValue(formPath(fieldPath), value as PathValue<EditCdcSinkTaskFormData, FormPath>, {
            shouldDirty: true,
            shouldValidate: true,
        });
    };

    return {
        handleSourceTableChange,
        sourceSchemaOptions,
        sourceTableOptions,
    };
}

function getEmbeddedPropertyName(
    selectedTable: CdcSinkSourceTable,
    relation: { foreignKey: CdcSinkSourceForeignKey; type: "selectedToParent" | "parentToSelected" }
) {
    if (relation?.type === "parentToSelected") {
        return relation.foreignKey.Columns.map(propertyNameFromJoinColumn).join("And");
    }

    return pascalCase(selectedTable.SourceTableName);
}

function getParentPath(path: AutoFillPath) {
    return path.split(".").slice(0, -2).join(".");
}

function formPath(path: string) {
    return path as FormPath;
}
