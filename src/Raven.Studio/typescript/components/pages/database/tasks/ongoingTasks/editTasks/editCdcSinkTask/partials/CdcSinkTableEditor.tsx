import { useFormContext, useFieldArray, useWatch } from "react-hook-form";
import { FormInput, FormGroup, FormLabel, FormSwitch, FormAceEditor } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { CdcSinkFormData, CdcSinkEmbeddedTableFormData, CdcSinkLinkedTableFormData, CdcSinkColumnType } from "../types";
import { useState } from "react";

interface CdcSinkTableEditorProps {
    tableIndex: number;
    onClose: () => void;
}

export default function CdcSinkTableEditor({ tableIndex, onClose }: CdcSinkTableEditorProps) {
    const { control, register, setValue, getValues } = useFormContext<CdcSinkFormData>();
    const prefix = `tables.${tableIndex}` as const;

    const tableValues = useWatch({ control, name: `tables.${tableIndex}` });

    return (
        <div>
            <div className="d-flex justify-content-between align-items-center mb-3">
                <h3>
                    <Icon icon="edit" /> Edit Table: {tableValues?.name || "(unnamed)"}
                </h3>
                <button type="button" className="btn btn-secondary" onClick={onClose}>
                    <Icon icon="arrow-left" /> Back to Table List
                </button>
            </div>

            <div className="card mb-3">
                <div className="card-body">
                    <h4>General</h4>

                    <FormGroup>
                        <FormLabel>Collection Name</FormLabel>
                        <FormInput
                            type="text"
                            control={control}
                            name={`tables.${tableIndex}.name`}
                            placeholder="RavenDB collection name"
                        />
                    </FormGroup>

                    <div className="row">
                        <div className="col-6">
                            <FormGroup>
                                <FormLabel>Source Schema</FormLabel>
                                <FormInput
                                    type="text"
                                    control={control}
                                    name={`tables.${tableIndex}.sourceTableSchema`}
                                    placeholder="dbo"
                                />
                            </FormGroup>
                        </div>
                        <div className="col-6">
                            <FormGroup>
                                <FormLabel>Source Table Name</FormLabel>
                                <FormInput
                                    type="text"
                                    control={control}
                                    name={`tables.${tableIndex}.sourceTableName`}
                                    placeholder="SQL table name"
                                />
                            </FormGroup>
                        </div>
                    </div>

                    <FormGroup>
                        <FormSwitch control={control} name={`tables.${tableIndex}.disabled`}>
                            Disable this table
                        </FormSwitch>
                    </FormGroup>
                </div>
            </div>

            <CdcSinkColumnMapping tableIndex={tableIndex} />

            <CdcSinkPrimaryKeyColumns tableIndex={tableIndex} />

            <div className="card mb-3">
                <div className="card-body">
                    <h4>Patch Script</h4>
                    <FormGroup>
                        <FormAceEditor
                            control={control}
                            name={`tables.${tableIndex}.patch`}
                            mode="javascript"
                            height="150px"
                        />
                    </FormGroup>
                </div>
            </div>

            <CdcSinkOnDeleteSection tableIndex={tableIndex} />

            <CdcSinkEmbeddedTablesSection tableIndex={tableIndex} />

            <CdcSinkLinkedTablesSection tableIndex={tableIndex} />
        </div>
    );
}

function CdcSinkColumnMapping({ tableIndex }: { tableIndex: number }) {
    const { control, getValues, setValue } = useFormContext<CdcSinkFormData>();
    const columns = useWatch({ control, name: `tables.${tableIndex}.columns` }) ?? [];
    const [newSqlColumn, setNewSqlColumn] = useState("");
    const [newTargetName, setNewTargetName] = useState("");
    const [newType, setNewType] = useState<CdcSinkColumnType>("Default");

    const handleAdd = () => {
        if (!newSqlColumn) return;
        const current = getValues(`tables.${tableIndex}.columns`) ?? [];
        setValue(
            `tables.${tableIndex}.columns`,
            [...current, { column: newSqlColumn, name: newTargetName || newSqlColumn, type: newType }],
            { shouldDirty: true, shouldValidate: true }
        );
        setNewSqlColumn("");
        setNewTargetName("");
        setNewType("Default");
    };

    const handleRemove = (index: number) => {
        const current = [...(getValues(`tables.${tableIndex}.columns`) ?? [])];
        current.splice(index, 1);
        setValue(`tables.${tableIndex}.columns`, current, { shouldDirty: true, shouldValidate: true });
    };

    return (
        <div className="card mb-3">
            <div className="card-body">
                <h4>Column Mapping</h4>
                <table className="table table-sm">
                    <thead>
                        <tr>
                            <th>SQL Column</th>
                            <th>Target Name</th>
                            <th style={{ width: "140px" }}>Type</th>
                            <th style={{ width: "60px" }}></th>
                        </tr>
                    </thead>
                    <tbody>
                        {columns.map((col, idx) => (
                            <tr key={idx}>
                                <td>{col.column}</td>
                                <td>{col.name}</td>
                                <td>{col.type}</td>
                                <td>
                                    <button
                                        type="button"
                                        className="btn btn-sm btn-outline-danger"
                                        onClick={() => handleRemove(idx)}
                                    >
                                        <Icon icon="trash" />
                                    </button>
                                </td>
                            </tr>
                        ))}
                        <tr>
                            <td>
                                <input
                                    type="text"
                                    className="form-control form-control-sm"
                                    placeholder="SQL column name"
                                    value={newSqlColumn}
                                    onChange={(e) => setNewSqlColumn(e.target.value)}
                                />
                            </td>
                            <td>
                                <input
                                    type="text"
                                    className="form-control form-control-sm"
                                    placeholder="Target name (defaults to column name)"
                                    value={newTargetName}
                                    onChange={(e) => setNewTargetName(e.target.value)}
                                />
                            </td>
                            <td>
                                <select
                                    className="form-select form-select-sm"
                                    value={newType}
                                    onChange={(e) => setNewType(e.target.value as CdcSinkColumnType)}
                                >
                                    <option value="Default">Default</option>
                                    <option value="Json">Json</option>
                                    <option value="Attachment">Attachment</option>
                                </select>
                            </td>
                            <td>
                                <button
                                    type="button"
                                    className="btn btn-sm btn-secondary"
                                    onClick={handleAdd}
                                    disabled={!newSqlColumn}
                                >
                                    <Icon icon="plus" />
                                </button>
                            </td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function CdcSinkPrimaryKeyColumns({ tableIndex }: { tableIndex: number }) {
    const { control, getValues, setValue } = useFormContext<CdcSinkFormData>();
    const primaryKeyColumns = useWatch({ control, name: `tables.${tableIndex}.primaryKeyColumns` }) ?? [];
    const [newColumn, setNewColumn] = useState("");

    const handleAdd = () => {
        if (!newColumn) return;
        const current = getValues(`tables.${tableIndex}.primaryKeyColumns`) ?? [];
        if (!current.includes(newColumn)) {
            setValue(`tables.${tableIndex}.primaryKeyColumns`, [...current, newColumn], {
                shouldDirty: true,
                shouldValidate: true,
            });
        }
        setNewColumn("");
    };

    const handleRemove = (col: string) => {
        const current = getValues(`tables.${tableIndex}.primaryKeyColumns`) ?? [];
        setValue(
            `tables.${tableIndex}.primaryKeyColumns`,
            current.filter((c) => c !== col),
            { shouldDirty: true, shouldValidate: true }
        );
    };

    return (
        <div className="card mb-3">
            <div className="card-body">
                <h4>Primary Key Columns</h4>
                <div className="d-flex flex-wrap gap-2 mb-2">
                    {primaryKeyColumns.map((col) => (
                        <span key={col} className="badge bg-primary d-flex align-items-center gap-1">
                            {col}
                            <button
                                type="button"
                                className="btn-close btn-close-white btn-sm"
                                onClick={() => handleRemove(col)}
                            />
                        </span>
                    ))}
                </div>
                <div className="d-flex gap-2">
                    <input
                        type="text"
                        className="form-control form-control-sm"
                        placeholder="Column name"
                        value={newColumn}
                        onChange={(e) => setNewColumn(e.target.value)}
                        onKeyDown={(e) => {
                            if (e.key === "Enter") {
                                e.preventDefault();
                                handleAdd();
                            }
                        }}
                    />
                    <button type="button" className="btn btn-sm btn-secondary" onClick={handleAdd} disabled={!newColumn}>
                        <Icon icon="plus" /> Add
                    </button>
                </div>
            </div>
        </div>
    );
}

function CdcSinkOnDeleteSection({ tableIndex }: { tableIndex: number }) {
    const { control, setValue, getValues } = useFormContext<CdcSinkFormData>();
    const onDelete = useWatch({ control, name: `tables.${tableIndex}.onDelete` });
    const hasOnDelete = onDelete != null;

    const toggleOnDelete = () => {
        if (hasOnDelete) {
            setValue(`tables.${tableIndex}.onDelete`, null, { shouldDirty: true });
        } else {
            setValue(`tables.${tableIndex}.onDelete`, { patch: "", ignoreDeletes: false }, { shouldDirty: true });
        }
    };

    return (
        <div className="card mb-3">
            <div className="card-body">
                <h4>On Delete</h4>
                <FormGroup>
                    <div className="form-check form-switch">
                        <input
                            type="checkbox"
                            className="form-check-input"
                            checked={hasOnDelete}
                            onChange={toggleOnDelete}
                            id={`onDelete-toggle-${tableIndex}`}
                        />
                        <label className="form-check-label" htmlFor={`onDelete-toggle-${tableIndex}`}>
                            Configure delete behavior
                        </label>
                    </div>
                </FormGroup>
                {hasOnDelete && (
                    <>
                        <FormGroup>
                            <FormSwitch control={control} name={`tables.${tableIndex}.onDelete.ignoreDeletes`}>
                                Ignore deletes
                            </FormSwitch>
                        </FormGroup>
                        <FormGroup>
                            <FormLabel>Delete Patch Script</FormLabel>
                            <FormAceEditor
                                control={control}
                                name={`tables.${tableIndex}.onDelete.patch`}
                                mode="javascript"
                                height="120px"
                            />
                        </FormGroup>
                    </>
                )}
            </div>
        </div>
    );
}

function CdcSinkEmbeddedTablesSection({ tableIndex }: { tableIndex: number }) {
    const { control } = useFormContext<CdcSinkFormData>();
    const { fields, append, remove } = useFieldArray({
        control,
        name: `tables.${tableIndex}.embeddedTables`,
    });

    const handleAddEmbeddedTable = () => {
        const newTable: CdcSinkEmbeddedTableFormData = {
            sourceTableSchema: "",
            sourceTableName: "",
            propertyName: "",
            type: "Array",
            joinColumns: [],
            primaryKeyColumns: [],
            columns: [],
            patch: "",
            onDelete: null,
            caseSensitiveKeys: false,
            embeddedTables: [],
        };
        append(newTable);
    };

    return (
        <div className="card mb-3">
            <div className="card-body">
                <div className="d-flex justify-content-between align-items-center mb-2">
                    <h4>Embedded Tables ({fields.length})</h4>
                    <button type="button" className="btn btn-sm btn-secondary" onClick={handleAddEmbeddedTable}>
                        <Icon icon="plus" /> Add Embedded Table
                    </button>
                </div>
                {fields.map((field, embIndex) => (
                    <EmbeddedTableRow
                        key={field.id}
                        tableIndex={tableIndex}
                        embIndex={embIndex}
                        onRemove={() => remove(embIndex)}
                    />
                ))}
            </div>
        </div>
    );
}

function EmbeddedTableRow({
    tableIndex,
    embIndex,
    onRemove,
}: {
    tableIndex: number;
    embIndex: number;
    onRemove: () => void;
}) {
    const { control } = useFormContext<CdcSinkFormData>();
    const prefix = `tables.${tableIndex}.embeddedTables.${embIndex}` as const;

    return (
        <div className="border rounded p-3 mb-2">
            <div className="d-flex justify-content-between align-items-center mb-2">
                <strong>Embedded Table #{embIndex + 1}</strong>
                <button type="button" className="btn btn-sm btn-outline-danger" onClick={onRemove}>
                    <Icon icon="trash" /> Remove
                </button>
            </div>
            <div className="row">
                <div className="col-4">
                    <FormGroup>
                        <FormLabel>Source Table</FormLabel>
                        <FormInput
                            type="text"
                            control={control}
                            name={`tables.${tableIndex}.embeddedTables.${embIndex}.sourceTableName`}
                            placeholder="Table name"
                        />
                    </FormGroup>
                </div>
                <div className="col-4">
                    <FormGroup>
                        <FormLabel>Property Name</FormLabel>
                        <FormInput
                            type="text"
                            control={control}
                            name={`tables.${tableIndex}.embeddedTables.${embIndex}.propertyName`}
                            placeholder="Property name"
                        />
                    </FormGroup>
                </div>
                <div className="col-4">
                    <FormGroup>
                        <FormLabel>Type</FormLabel>
                        <select
                            className="form-select form-select-sm"
                            {...control.register(`tables.${tableIndex}.embeddedTables.${embIndex}.type`)}
                        >
                            <option value="Array">Array</option>
                            <option value="Map">Map</option>
                            <option value="Value">Value</option>
                        </select>
                    </FormGroup>
                </div>
            </div>
        </div>
    );
}

function CdcSinkLinkedTablesSection({ tableIndex }: { tableIndex: number }) {
    const { control } = useFormContext<CdcSinkFormData>();
    const { fields, append, remove } = useFieldArray({
        control,
        name: `tables.${tableIndex}.linkedTables`,
    });

    const handleAddLinkedTable = () => {
        const newTable: CdcSinkLinkedTableFormData = {
            sourceTableSchema: "",
            sourceTableName: "",
            propertyName: "",
            linkedCollectionName: "",
            type: "Value",
            joinColumns: [],
        };
        append(newTable);
    };

    return (
        <div className="card mb-3">
            <div className="card-body">
                <div className="d-flex justify-content-between align-items-center mb-2">
                    <h4>Linked Tables ({fields.length})</h4>
                    <button type="button" className="btn btn-sm btn-secondary" onClick={handleAddLinkedTable}>
                        <Icon icon="plus" /> Add Linked Table
                    </button>
                </div>
                {fields.map((field, linkIndex) => (
                    <LinkedTableRow
                        key={field.id}
                        tableIndex={tableIndex}
                        linkIndex={linkIndex}
                        onRemove={() => remove(linkIndex)}
                    />
                ))}
            </div>
        </div>
    );
}

function LinkedTableRow({
    tableIndex,
    linkIndex,
    onRemove,
}: {
    tableIndex: number;
    linkIndex: number;
    onRemove: () => void;
}) {
    const { control } = useFormContext<CdcSinkFormData>();

    return (
        <div className="border rounded p-3 mb-2">
            <div className="d-flex justify-content-between align-items-center mb-2">
                <strong>Linked Table #{linkIndex + 1}</strong>
                <button type="button" className="btn btn-sm btn-outline-danger" onClick={onRemove}>
                    <Icon icon="trash" /> Remove
                </button>
            </div>
            <div className="row">
                <div className="col-3">
                    <FormGroup>
                        <FormLabel>Source Table</FormLabel>
                        <FormInput
                            type="text"
                            control={control}
                            name={`tables.${tableIndex}.linkedTables.${linkIndex}.sourceTableName`}
                            placeholder="Table name"
                        />
                    </FormGroup>
                </div>
                <div className="col-3">
                    <FormGroup>
                        <FormLabel>Property Name</FormLabel>
                        <FormInput
                            type="text"
                            control={control}
                            name={`tables.${tableIndex}.linkedTables.${linkIndex}.propertyName`}
                            placeholder="Property name"
                        />
                    </FormGroup>
                </div>
                <div className="col-3">
                    <FormGroup>
                        <FormLabel>Linked Collection</FormLabel>
                        <FormInput
                            type="text"
                            control={control}
                            name={`tables.${tableIndex}.linkedTables.${linkIndex}.linkedCollectionName`}
                            placeholder="Collection name"
                        />
                    </FormGroup>
                </div>
                <div className="col-3">
                    <FormGroup>
                        <FormLabel>Type</FormLabel>
                        <select
                            className="form-select form-select-sm"
                            {...control.register(`tables.${tableIndex}.linkedTables.${linkIndex}.type`)}
                        >
                            <option value="Array">Array</option>
                            <option value="Value">Value</option>
                        </select>
                    </FormGroup>
                </div>
            </div>
        </div>
    );
}
