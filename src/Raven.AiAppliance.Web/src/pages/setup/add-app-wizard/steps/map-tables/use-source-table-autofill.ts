import type { DiscoverForeignKeyResponse, DiscoverTableResponse } from "@/api/generated/server-api";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import type { MapTablePath } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";
import {
    findDiscoveredTable,
    getDiscoveredSchemaNames,
    getDiscoveredTableNames,
    getForeignKeysToTable,
    mapDiscoveredColumns,
    pascalCase,
    propertyNameFromJoinColumn,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { useFormContext, useWatch, type FieldPath, type PathValue } from "react-hook-form";

type AutofillMode = "root" | "embedded" | "linked";
type FormPath = FieldPath<AppFormData>;

/**
 * Suggests source schemas/tables from the discovered schema and, when the user picks a
 * discovered table, pre-fills the empty parts of the mapping (columns, keys, names).
 */
export function useSourceTableAutofill(path: MapTablePath, mode: AutofillMode) {
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);
    const { control, getValues, setValue } = useFormContext<AppFormData>();

    const sourceTableSchema = useWatch({ control, name: `${path}.sourceTableSchema` });

    const schemaOptions = getDiscoveredSchemaNames(discoverResult);
    const tableOptions = getDiscoveredTableNames(discoverResult, sourceTableSchema);

    const setFieldValue = (fieldPath: string, value: unknown) => {
        setValue(fieldPath as FormPath, value as PathValue<AppFormData, FormPath>, {
            shouldDirty: true,
            shouldValidate: true,
        });
    };

    const setStringIfEmpty = (fieldPath: string, value: string | null) => {
        const currentValue = getValues(fieldPath as FormPath);

        if (!String(currentValue ?? "").trim()) {
            setFieldValue(fieldPath, value);
        }
    };

    const setListIfEmpty = (fieldPath: string, value: unknown[]) => {
        const currentValue = getValues(fieldPath as FormPath) as unknown[] | undefined;

        if (!currentValue?.length && value.length > 0) {
            setFieldValue(fieldPath, value);
        }
    };

    const getParentDiscoveredTable = () => {
        const parentPath = path.split(".").slice(0, -2).join(".");
        const parentTable = getValues(parentPath as FormPath) as {
            sourceTableSchema?: string | null;
            sourceTableName?: string | null;
        } | null;

        return findDiscoveredTable(discoverResult, parentTable?.sourceTableSchema, parentTable?.sourceTableName);
    };

    // The relation between the parent table and the selected child, but only when it is
    // unambiguous (exactly one foreign key in either direction).
    const getSingleRelation = (selectedTable: DiscoverTableResponse) => {
        const parentTable = getParentDiscoveredTable();

        if (!parentTable) {
            return null;
        }

        const selectedToParent = getForeignKeysToTable(selectedTable, parentTable).map((foreignKey) => ({
            foreignKey,
            direction: "selectedToParent" as const,
        }));
        const parentToSelected = getForeignKeysToTable(parentTable, selectedTable).map((foreignKey) => ({
            foreignKey,
            direction: "parentToSelected" as const,
        }));
        const relations = [...selectedToParent, ...parentToSelected];

        return relations.length === 1 ? relations[0] : null;
    };

    const fillRootTable = (selectedTable: DiscoverTableResponse) => {
        setStringIfEmpty(`${path}.collectionName`, pascalCase(selectedTable.sourceTableName));
        setListIfEmpty(`${path}.primaryKeyColumns`, [...selectedTable.primaryKeyColumns]);
        setListIfEmpty(`${path}.columns`, mapDiscoveredColumns(selectedTable));
    };

    const fillEmbeddedTable = (selectedTable: DiscoverTableResponse) => {
        const relation = getSingleRelation(selectedTable);

        setStringIfEmpty(`${path}.propertyName`, getEmbeddedPropertyName(selectedTable, relation));
        setListIfEmpty(`${path}.primaryKeyColumns`, [...selectedTable.primaryKeyColumns]);
        setListIfEmpty(`${path}.columns`, mapDiscoveredColumns(selectedTable));

        if (relation) {
            setListIfEmpty(`${path}.joinColumns`, [...relation.foreignKey.columns]);
        }
    };

    const fillLinkedTable = (selectedTable: DiscoverTableResponse) => {
        const parentTable = getParentDiscoveredTable();
        const relations = parentTable ? getForeignKeysToTable(parentTable, selectedTable) : [];
        const relation = relations.length === 1 ? relations[0] : null;

        setStringIfEmpty(`${path}.linkedCollectionName`, pascalCase(selectedTable.sourceTableName));

        if (relation) {
            setStringIfEmpty(`${path}.propertyName`, relation.columns.map(propertyNameFromJoinColumn).join("And"));
            setListIfEmpty(`${path}.joinColumns`, [...relation.columns]);
        }
    };

    const handleSourceTableChange = (sourceTableName: string) => {
        // With no schema picked yet, match the table name in any schema and backfill the schema.
        const selectedTable = sourceTableSchema?.trim()
            ? findDiscoveredTable(discoverResult, sourceTableSchema, sourceTableName)
            : discoverResult?.tables.find((table) => table.sourceTableName === sourceTableName);

        if (!selectedTable) {
            return;
        }

        setStringIfEmpty(`${path}.sourceTableSchema`, selectedTable.sourceTableSchema ?? null);

        if (mode === "root") {
            fillRootTable(selectedTable);
        } else if (mode === "embedded") {
            fillEmbeddedTable(selectedTable);
        } else {
            fillLinkedTable(selectedTable);
        }
    };

    return {
        schemaOptions,
        tableOptions,
        handleSourceTableChange,
    };
}

function getEmbeddedPropertyName(
    selectedTable: DiscoverTableResponse,
    relation: { foreignKey: DiscoverForeignKeyResponse; direction: "selectedToParent" | "parentToSelected" } | null,
) {
    if (relation?.direction === "parentToSelected") {
        return relation.foreignKey.columns.map(propertyNameFromJoinColumn).join("And");
    }

    return pascalCase(selectedTable.sourceTableName);
}
