import "./TableDisplaySettings.scss";
import { Table as TanstackTable } from "@tanstack/react-table";
import { Checkbox } from "components/common/Checkbox";
import { Icon } from "components/common/Icon";
import {
    ColumnMeta,
    useTableDisplaySettings,
} from "components/common/virtualTable/commonComponents/columnsSelect/useTableDisplaySettings";
import {
    createCustomColumnId,
    CustomColumnDefinition,
    getCustomColumnExpressionError,
} from "components/common/virtualTable/commonComponents/columnsSelect/customColumns";
import { ClassNameProps } from "components/models/common";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { useViewSheet, ViewSheet } from "components/common/splitView/ViewSheet";
import {
    closestCenter,
    DndContext,
    DndContextProps,
    DragOverlay,
    PointerSensor,
    useSensor,
    useSensors,
} from "@dnd-kit/core";
import { arrayMove, SortableContext, useSortable, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { CSSProperties, useState } from "react";
import genUtils from "common/generalUtils";
import Card from "react-bootstrap/Card";

// when provided, the sheet lets the user add, edit and remove columns defined by a JavaScript expression
export interface CustomColumnsSettings {
    columns: CustomColumnDefinition[];
    onChange: (columns: CustomColumnDefinition[]) => void;
}

export interface AppliedColumnLayout {
    visibleColumnIds: string[];
    columnOrder: string[];
    pinnedColumnIds: string[];
    customColumns: CustomColumnDefinition[];
}

export interface TableDisplaySettingsOptions {
    customColumns?: CustomColumnsSettings;
    // called with the layout after it is applied to the table, e.g. to persist it
    onApplied?: (layout: AppliedColumnLayout) => void;
    // when provided, "Restart to default" delegates to the consumer instead of reverting the sheet to its initial state
    onRestoreDefaults?: () => void;
}

interface TableDisplaySettingsProps<T> extends ClassNameProps, TableDisplaySettingsOptions {
    table: TanstackTable<T>;
}

export function useTableDisplaySettingsSheet<T>(table: TanstackTable<T>, options: TableDisplaySettingsOptions = {}) {
    const { customColumns, onApplied, onRestoreDefaults } = options;
    const { open } = useViewSheet();
    const {
        columnMetas,
        allColumnIds,
        getInitialColumnOrder,
        getInitialPinnedIds,
        getInitialSelectedIds,
        applySettings,
    } = useTableDisplaySettings(table);

    const openSheet = () => {
        open({
            component: (
                <TableDisplaySettingsSheet
                    columnMetas={columnMetas}
                    allColumnIds={allColumnIds}
                    initialSelectedIds={getInitialSelectedIds()}
                    initialColumnOrder={getInitialColumnOrder()}
                    initialPinnedIds={getInitialPinnedIds()}
                    customColumns={customColumns}
                    onApply={(selectedIds, columnOrder, pinnedIds, customColumnList) => {
                        applySettings(selectedIds, columnOrder, pinnedIds);
                        customColumns?.onChange(customColumnList);
                        onApplied?.({
                            visibleColumnIds: selectedIds,
                            columnOrder,
                            pinnedColumnIds: pinnedIds,
                            customColumns: customColumnList,
                        });
                    }}
                    onRestoreDefaults={onRestoreDefaults}
                />
            ),
            initialWidth: 400,
            minWidth: "20%",
            maxWidth: "50%",
            isPinned: false,
        });
    };

    return { openSheet };
}

export default function TableDisplaySettings<T>({ table, className, ...options }: TableDisplaySettingsProps<T>) {
    const { openSheet } = useTableDisplaySettingsSheet(table, options);

    return (
        <div className={classNames("table-display-settings", className)}>
            <Button variant="secondary" onClick={openSheet}>
                <Icon icon="table" />
                Column layout settings
            </Button>
        </div>
    );
}

interface TableDisplaySettingsSheetProps {
    columnMetas: ColumnMeta[];
    allColumnIds: string[];
    initialSelectedIds: string[];
    initialColumnOrder: string[];
    initialPinnedIds: string[];
    customColumns?: CustomColumnsSettings;
    onApply: (
        selectedIds: string[],
        columnOrder: string[],
        pinnedIds: string[],
        customColumns: CustomColumnDefinition[]
    ) => void;
    onRestoreDefaults?: () => void;
}

function TableDisplaySettingsSheet({
    columnMetas,
    allColumnIds,
    initialSelectedIds,
    initialColumnOrder,
    initialPinnedIds,
    customColumns,
    onApply,
    onRestoreDefaults,
}: TableDisplaySettingsSheetProps) {
    const { close } = useViewSheet();

    const initialCustomColumns = customColumns?.columns ?? [];

    const [columnOrder, setColumnOrder] = useState<string[]>(initialColumnOrder);
    const [selectedIds, setSelectedIds] = useState<string[]>(initialSelectedIds);
    const [pinnedIds, setPinnedIds] = useState<string[]>(initialPinnedIds);
    const [customColumnList, setCustomColumnList] = useState<CustomColumnDefinition[]>(initialCustomColumns);
    const [editedCustomColumn, setEditedCustomColumn] = useState<CustomColumnDefinition>(null);
    const [activeDragId, setActiveDragId] = useState<string>(null);

    const sensors = useSensors(useSensor(PointerSensor));

    const customColumnIds = customColumnList.map((x) => x.id);
    const removedCustomColumnIds = initialCustomColumns.map((x) => x.id).filter((id) => !customColumnIds.includes(id));

    const metaById: Record<string, ColumnMeta> = {
        ...Object.fromEntries(columnMetas.map((m) => [m.id, m])),
        ...Object.fromEntries(
            customColumnList.map((column): [string, ColumnMeta] => [
                column.id,
                { id: column.id, headerTitle: column.header, canHide: true, canPin: true, customColumn: column },
            ])
        ),
    };

    const availableColumnIds = [...allColumnIds, ...customColumnIds.filter((id) => !allColumnIds.includes(id))].filter(
        (id) => !removedCustomColumnIds.includes(id)
    );

    const hideableIds = availableColumnIds.filter((id) => metaById[id].canHide);
    const selectionState = genUtils.getSelectionState(
        hideableIds,
        selectedIds.filter((id) => hideableIds.includes(id))
    );

    const orderedIds = columnOrder.filter((id) => availableColumnIds.includes(id));
    const pinnedColumnIds = orderedIds.filter((id) => pinnedIds.includes(id));
    const unpinnedColumnIds = orderedIds.filter((id) => !pinnedIds.includes(id));

    const handleToggleAll = () => {
        if (selectionState === "Empty") {
            setSelectedIds(availableColumnIds);
        } else {
            setSelectedIds(selectedIds.filter((id) => !hideableIds.includes(id)));
        }
    };

    const handleToggleOne = (id: string) => {
        setSelectedIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
    };

    const handleTogglePin = (id: string) => {
        setPinnedIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
    };

    const handleReset = () => {
        if (onRestoreDefaults) {
            onRestoreDefaults();
            close();
            return;
        }

        setColumnOrder(initialColumnOrder);
        setSelectedIds(initialSelectedIds);
        setPinnedIds(initialPinnedIds);
        setCustomColumnList(initialCustomColumns);
        setEditedCustomColumn(null);
    };

    const handleApply = () => {
        onApply(selectedIds, columnOrder, pinnedIds, customColumnList);
        close();
    };

    const handleSaveCustomColumn = (column: CustomColumnDefinition) => {
        const isNew = !customColumnList.some((x) => x.id === column.id);

        setCustomColumnList((prev) => (isNew ? [...prev, column] : prev.map((x) => (x.id === column.id ? column : x))));

        if (isNew) {
            setColumnOrder((prev) => [...prev, column.id]);
            setSelectedIds((prev) => [...prev, column.id]);
        }

        setEditedCustomColumn(null);
    };

    const handleRemoveCustomColumn = (id: string) => {
        setCustomColumnList((prev) => prev.filter((x) => x.id !== id));
        setColumnOrder((prev) => prev.filter((x) => x !== id));
        setSelectedIds((prev) => prev.filter((x) => x !== id));
        setPinnedIds((prev) => prev.filter((x) => x !== id));
    };

    const handleDragStart: DndContextProps["onDragStart"] = (event) => {
        setActiveDragId(String(event.active.id));
    };

    const handleDragEnd = (event: Parameters<DndContextProps["onDragEnd"]>[0], isPinnedSection: boolean) => {
        const { active, over } = event;
        if (over && active.id !== over.id) {
            const activeId = String(active.id);
            const overId = String(over.id);
            setColumnOrder((items) => {
                const sectionIds = isPinnedSection
                    ? items.filter((id) => pinnedIds.includes(id))
                    : items.filter((id) => !pinnedIds.includes(id));
                const oldIndex = sectionIds.indexOf(activeId);
                const newIndex = sectionIds.indexOf(overId);
                if (oldIndex === -1 || newIndex === -1) {
                    return items;
                }
                const reorderedSection = arrayMove(sectionIds, oldIndex, newIndex);
                const allPinned = items.filter((id) => pinnedIds.includes(id));
                const allUnpinned = items.filter((id) => !pinnedIds.includes(id));
                const newPinned = isPinnedSection ? reorderedSection : allPinned;
                const newUnpinned = isPinnedSection ? allUnpinned : reorderedSection;
                return [...newPinned, ...newUnpinned];
            });
        }
        setActiveDragId(null);
    };

    const handleDragCancel = () => {
        setActiveDragId(null);
    };

    const renderSortableRow = (id: string, isPinned: boolean) => (
        <SortableColumnRow
            key={id}
            id={id}
            meta={metaById[id]}
            isSelected={selectedIds.includes(id)}
            isPinned={isPinned}
            isDraggingActive={activeDragId !== null}
            onToggle={() => handleToggleOne(id)}
            onTogglePin={() => handleTogglePin(id)}
            onEdit={() => setEditedCustomColumn(metaById[id].customColumn)}
            onRemove={() => handleRemoveCustomColumn(id)}
        />
    );

    return (
        <ViewSheet>
            <ViewSheet.Header>
                <h3 className="mb-0">
                    <Icon icon="table" color="primary" />
                    Column layout settings
                </h3>
            </ViewSheet.Header>
            <ViewSheet.Body className="m-2">
                <h4 className="mb-2">Set up your column layout</h4>
                <Card className="bg-black p-1">
                    {hideableIds.length > 0 && (
                        <div className="px-2 py-1 d-flex align-items-center border-bottom border-secondary">
                            <Checkbox
                                selected={selectionState === "AllSelected"}
                                toggleSelection={handleToggleAll}
                                indeterminate={selectionState === "SomeSelected"}
                                color="primary"
                                title="Select all"
                            >
                                Select all
                            </Checkbox>
                        </div>
                    )}
                    <div className="column-list">
                        {pinnedColumnIds.length > 0 && (
                            <>
                                <div className="label d-flex p-2 align-items-center">
                                    <Icon icon="pinned" />
                                    Pinned
                                </div>
                                <DndContext
                                    id="pinned-dnd"
                                    sensors={sensors}
                                    collisionDetection={closestCenter}
                                    onDragStart={handleDragStart}
                                    onDragEnd={(event) => handleDragEnd(event, true)}
                                    onDragCancel={handleDragCancel}
                                >
                                    <SortableContext items={pinnedColumnIds} strategy={verticalListSortingStrategy}>
                                        {pinnedColumnIds.map((id) => renderSortableRow(id, true))}
                                    </SortableContext>
                                    <DragOverlay>
                                        {activeDragId && pinnedIds.includes(activeDragId) ? (
                                            <ColumnRowPreview
                                                id={activeDragId}
                                                meta={metaById[activeDragId]}
                                                isSelected={selectedIds.includes(activeDragId)}
                                                isPinned
                                            />
                                        ) : null}
                                    </DragOverlay>
                                </DndContext>
                                <hr className="my-0" />
                            </>
                        )}
                        <DndContext
                            id="unpinned-dnd"
                            sensors={sensors}
                            collisionDetection={closestCenter}
                            onDragStart={handleDragStart}
                            onDragEnd={(event) => handleDragEnd(event, false)}
                            onDragCancel={handleDragCancel}
                        >
                            <SortableContext items={unpinnedColumnIds} strategy={verticalListSortingStrategy}>
                                {unpinnedColumnIds.map((id) => renderSortableRow(id, false))}
                            </SortableContext>
                            <DragOverlay>
                                {activeDragId && !pinnedIds.includes(activeDragId) ? (
                                    <ColumnRowPreview
                                        id={activeDragId}
                                        meta={metaById[activeDragId]}
                                        isSelected={selectedIds.includes(activeDragId)}
                                        isPinned={false}
                                    />
                                ) : null}
                            </DragOverlay>
                        </DndContext>
                    </div>
                </Card>
                {customColumns && (
                    <div className="mt-3">
                        {editedCustomColumn ? (
                            <CustomColumnForm
                                column={editedCustomColumn}
                                onSave={handleSaveCustomColumn}
                                onCancel={() => setEditedCustomColumn(null)}
                            />
                        ) : (
                            <Button
                                variant="primary"
                                size="sm"
                                onClick={() =>
                                    setEditedCustomColumn({ id: createCustomColumnId(), header: "", expression: "" })
                                }
                            >
                                <Icon icon="plus" />
                                Add a custom column
                            </Button>
                        )}
                    </div>
                )}
            </ViewSheet.Body>
            <ViewSheet.Footer>
                <div className="d-flex justify-content-between w-100">
                    <Button variant="outline" title="Restart to default" onClick={handleReset}>
                        <Icon icon="reset" />
                        Restart to default
                    </Button>
                    <Button title="Apply changes" onClick={handleApply} className="rounded-pill">
                        <Icon icon="save" />
                        Apply
                    </Button>
                </div>
            </ViewSheet.Footer>
        </ViewSheet>
    );
}

interface CustomColumnFormProps {
    column: CustomColumnDefinition;
    onSave: (column: CustomColumnDefinition) => void;
    onCancel: () => void;
}

function CustomColumnForm({ column, onSave, onCancel }: CustomColumnFormProps) {
    const [expression, setExpression] = useState(column.expression);
    const [header, setHeader] = useState(column.header);
    const [isSubmitted, setIsSubmitted] = useState(false);

    const expressionError = expression.trim()
        ? getCustomColumnExpressionError(expression)
        : "The binding expression is required";
    const headerError = header.trim() ? null : "The alias is required";

    const handleSave = () => {
        setIsSubmitted(true);

        if (expressionError || headerError) {
            return;
        }

        onSave({ id: column.id, header: header.trim(), expression: expression.trim() });
    };

    return (
        <Card className="bg-black p-2 vstack gap-2" data-testid="custom-column-form">
            <Form.Group>
                <Form.Label htmlFor="custom-column-expression" className="mb-1">
                    Binding expression
                </Form.Label>
                <Form.Control
                    id="custom-column-expression"
                    size="sm"
                    placeholder="e.g. this.ShipTo.City"
                    value={expression}
                    autoFocus
                    isInvalid={isSubmitted && !!expressionError}
                    onChange={(e) => setExpression(e.target.value)}
                />
                {isSubmitted && expressionError && (
                    <Form.Control.Feedback type="invalid">{expressionError}</Form.Control.Feedback>
                )}
            </Form.Group>
            <Form.Group>
                <Form.Label htmlFor="custom-column-alias" className="mb-1">
                    Alias
                </Form.Label>
                <Form.Control
                    id="custom-column-alias"
                    size="sm"
                    placeholder="Column name"
                    value={header}
                    isInvalid={isSubmitted && !!headerError}
                    onChange={(e) => setHeader(e.target.value)}
                />
                {isSubmitted && headerError && (
                    <Form.Control.Feedback type="invalid">{headerError}</Form.Control.Feedback>
                )}
            </Form.Group>
            <div className="d-flex gap-2 justify-content-end">
                <Button variant="secondary" size="sm" onClick={onCancel}>
                    <Icon icon="cancel" />
                    Cancel
                </Button>
                <Button variant="success" size="sm" onClick={handleSave}>
                    <Icon icon="check" />
                    Save column
                </Button>
            </div>
        </Card>
    );
}

interface ColumnRowProps {
    id: string;
    meta: ColumnMeta;
    isSelected: boolean;
    isPinned: boolean;
    onToggle?: () => void;
    onTogglePin?: () => void;
    onEdit?: () => void;
    onRemove?: () => void;
}

interface SortableColumnRowProps extends ColumnRowProps {
    isDraggingActive: boolean;
}

function SortableColumnRow({
    id,
    meta,
    isSelected,
    isPinned,
    isDraggingActive,
    onToggle,
    onTogglePin,
    onEdit,
    onRemove,
}: SortableColumnRowProps) {
    const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

    const style: CSSProperties = {
        transform: CSS.Transform.toString(transform),
        transition,
        opacity: isDragging ? 0.4 : 1,
        zIndex: isDragging ? 1 : undefined,
    };

    return (
        <div ref={setNodeRef} style={style} className="column-list-item d-flex align-items-center">
            <span
                className="column-drag-handle"
                title="Drag to reorder"
                {...attributes}
                {...listeners}
                style={{ cursor: isDragging || isDraggingActive ? (isDragging ? "grabbing" : "grab") : "grab" }}
            >
                <Icon icon="reorder" color="secondary" margin="m-0" />
            </span>
            <Checkbox
                selected={isSelected}
                toggleSelection={meta.canHide ? onToggle : undefined}
                disabled={!meta.canHide}
                title={meta.canHide ? meta.headerTitle : "This column is always visible and cannot be hidden"}
                className="flex-grow-1 overflow-hidden"
            >
                <span className="column-list-item-name">{meta.headerTitle}</span>
            </Checkbox>
            {meta.customColumn && (
                <>
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 flex-shrink-0 text-reset"
                        title="Edit custom column"
                        onClick={onEdit}
                    >
                        <Icon icon="edit" margin="m-0" />
                    </Button>
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 flex-shrink-0 text-reset"
                        title="Remove custom column"
                        onClick={onRemove}
                    >
                        <Icon icon="trash" margin="m-0" />
                    </Button>
                </>
            )}
            <Button
                variant="link"
                size="sm"
                className={classNames("p-0 flex-shrink-0", isPinned ? "text-primary" : "text-reset")}
                title={!meta.canPin ? "This column cannot be pinned" : isPinned ? "Unpin column" : "Pin column to left"}
                onClick={meta.canPin ? onTogglePin : undefined}
                disabled={!meta.canPin}
            >
                <Icon icon={isPinned ? "pinned" : "pin"} margin="m-0" />
            </Button>
        </div>
    );
}

function ColumnRowPreview({ meta, isSelected, isPinned }: ColumnRowProps) {
    return (
        <div className="column-list-item column-list-item-preview d-flex align-items-center">
            <span className="column-drag-handle" style={{ cursor: "grabbing" }}>
                <Icon icon="reorder" margin="m-0" />
            </span>
            <Checkbox
                selected={isSelected}
                toggleSelection={() => {}}
                disabled={!meta.canHide}
                title={meta.canHide ? meta.headerTitle : "This column is always visible and cannot be hidden"}
                className="flex-grow-1 overflow-hidden"
            >
                <span className="column-list-item-name">{meta.headerTitle}</span>
            </Checkbox>
            <Button
                variant="link"
                size="sm"
                className={classNames("p-0 flex-shrink-0", isPinned ? "text-primary" : "text-reset")}
                title={!meta.canPin ? "This column cannot be pinned" : isPinned ? "Unpin column" : "Pin column to left"}
                disabled={!meta.canPin}
            >
                <Icon icon={isPinned ? "pinned" : "pin"} margin="m-0" />
            </Button>
        </div>
    );
}
