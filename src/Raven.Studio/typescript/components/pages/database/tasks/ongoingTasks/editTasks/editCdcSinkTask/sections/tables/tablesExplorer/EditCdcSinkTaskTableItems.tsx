import { EmptySet } from "components/common/EmptySet";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import _ from "lodash";
import { useMemo } from "react";
import { UseFieldArrayReturn } from "react-hook-form";
import { EditCdcSinkTaskRootTableItem } from "./EditCdcSinkTaskRootTableItem";

interface EditCdcSinkTaskTableItemsProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
    filter: string;
}

export function EditCdcSinkTaskTableItems({ tablesFieldArray, filter }: EditCdcSinkTaskTableItemsProps) {
    const filteredGroupedTables = useMemo(() => {
        const normalizedFilter = filter.trim().toLowerCase();

        return Object.entries(_.groupBy(tablesFieldArray.fields, (table) => table.sourceTableSchema || "default"))
            .map(([schema, tables]) => ({
                schema,
                tables: normalizedFilter
                    ? tables.filter((table) => table.sourceTableName.toLowerCase().includes(normalizedFilter))
                    : tables,
            }))
            .filter((group) => group.tables.length > 0);
    }, [filter, tablesFieldArray.fields]);

    if (tablesFieldArray.fields.length === 0) {
        return <EmptySet compact>Use the Schema Explorer to discover existing tables or add new manually.</EmptySet>;
    }

    if (filteredGroupedTables.length === 0) {
        return <EmptySet compact>No tables match the filter.</EmptySet>;
    }

    return (
        <div className="vstack gap-1 overflow-y-auto flex-grow-0">
            {filteredGroupedTables.map(({ schema, tables }) => (
                <div key={schema} className="vstack gap-1">
                    <div className="text-center font-monospace small">{schema}</div>
                    {tables.map((table) => (
                        <EditCdcSinkTaskRootTableItem
                            key={table.id}
                            formIdx={tablesFieldArray.fields.findIndex((t) => t.id === table.id)!}
                        />
                    ))}
                </div>
            ))}
        </div>
    );
}
