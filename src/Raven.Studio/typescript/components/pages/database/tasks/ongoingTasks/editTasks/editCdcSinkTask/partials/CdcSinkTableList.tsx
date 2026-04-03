import { useFieldArray, useFormContext } from "react-hook-form";
import { CdcSinkFormData, CdcSinkTableFormData } from "../types";
import { Icon } from "components/common/Icon";
import { useState } from "react";

interface CdcSinkTableListProps {
    onEditTable: (index: number) => void;
}

export default function CdcSinkTableList({ onEditTable }: CdcSinkTableListProps) {
    const { control } = useFormContext<CdcSinkFormData>();
    const { fields, append, remove } = useFieldArray({ control, name: "tables" });

    const [confirmRemoveIndex, setConfirmRemoveIndex] = useState<number | null>(null);

    const handleAddManualTable = () => {
        const newTable: CdcSinkTableFormData = {
            name: "",
            sourceTableSchema: "",
            sourceTableName: "",
            columns: [],
            primaryKeyColumns: [],
            patch: "",
            onDelete: null,
            disabled: false,
            embeddedTables: [],
            linkedTables: [],
        };
        append(newTable);
        onEditTable(fields.length);
    };

    const handleRemove = (index: number) => {
        remove(index);
        setConfirmRemoveIndex(null);
    };

    return (
        <div>
            <div className="d-flex justify-content-between align-items-center mb-3">
                <h3>
                    <Icon icon="table" /> Configured Tables ({fields.length})
                </h3>
                <button type="button" className="btn btn-secondary" onClick={handleAddManualTable}>
                    <Icon icon="plus" /> Add Table Manually
                </button>
            </div>

            {fields.length === 0 ? (
                <div className="text-center p-4 text-muted border rounded">
                    No tables configured yet. Use Schema Explorer to discover tables, or add a table manually.
                </div>
            ) : (
                <div className="vstack gap-2">
                    {fields.map((field, index) => {
                        const table = field as unknown as CdcSinkTableFormData;
                        const columnCount = table.columns?.length ?? 0;
                        const embeddedCount = table.embeddedTables?.length ?? 0;
                        const linkedCount = table.linkedTables?.length ?? 0;

                        return (
                            <div key={field.id} className="card">
                                <div className="card-body d-flex justify-content-between align-items-center">
                                    <div>
                                        <div className="fw-bold">
                                            {table.name || "(unnamed collection)"}
                                        </div>
                                        <div className="text-muted small">
                                            Source: {table.sourceTableSchema ? `${table.sourceTableSchema}.` : ""}
                                            {table.sourceTableName || "(no source table)"}
                                        </div>
                                        <div className="text-muted small">
                                            {columnCount} column{columnCount !== 1 ? "s" : ""}
                                            {embeddedCount > 0 && (
                                                <span> | {embeddedCount} embedded</span>
                                            )}
                                            {linkedCount > 0 && (
                                                <span> | {linkedCount} linked</span>
                                            )}
                                            {table.disabled && (
                                                <span className="badge bg-warning ms-2">Disabled</span>
                                            )}
                                        </div>
                                    </div>
                                    <div className="d-flex gap-2">
                                        <button
                                            type="button"
                                            className="btn btn-sm btn-secondary"
                                            onClick={() => onEditTable(index)}
                                        >
                                            <Icon icon="edit" /> Edit
                                        </button>
                                        {confirmRemoveIndex === index ? (
                                            <div className="d-flex gap-1">
                                                <button
                                                    type="button"
                                                    className="btn btn-sm btn-danger"
                                                    onClick={() => handleRemove(index)}
                                                >
                                                    Confirm
                                                </button>
                                                <button
                                                    type="button"
                                                    className="btn btn-sm btn-secondary"
                                                    onClick={() => setConfirmRemoveIndex(null)}
                                                >
                                                    Cancel
                                                </button>
                                            </div>
                                        ) : (
                                            <button
                                                type="button"
                                                className="btn btn-sm btn-outline-danger"
                                                onClick={() => setConfirmRemoveIndex(index)}
                                            >
                                                <Icon icon="trash" /> Remove
                                            </button>
                                        )}
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}
