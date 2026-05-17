import "./TableDisplaySettings.scss";
import { Table as TanstackTable } from "@tanstack/react-table";
import { Checkbox } from "components/common/Checkbox";
import { Icon } from "components/common/Icon";
import {
    ColumnMeta,
    useTableDisplaySettings,
} from "components/common/virtualTable/commonComponents/columnsSelect/useTableDisplaySettings";
import { ClassNameProps } from "components/models/common";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
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

interface TableColumnsSelectProps<T> extends ClassNameProps {
    table: TanstackTable<T>;
}

export default function TableDisplaySettings<T>({ table, className }: TableColumnsSelectProps<T>) {
    const { open } = useViewSheet();
    const {
        columnMetas,
        allColumnIds,
        getInitialColumnOrder,
        getInitialPinnedIds,
        getInitialSelectedIds,
        applySettings,
    } = useTableDisplaySettings(table);

    const handleOpenSheet = () => {
        open({
            component: (
                <TableDisplaySettingsSheet
                    columnMetas={columnMetas}
                    allColumnIds={allColumnIds}
                    initialSelectedIds={getInitialSelectedIds()}
                    initialColumnOrder={getInitialColumnOrder()}
                    initialPinnedIds={getInitialPinnedIds()}
                    onApply={applySettings}
                />
            ),
            initialWidth: "30%",
            minWidth: "20%",
            maxWidth: "50%",
            isPinned: false,
        });
    };

    return (
        <div className={classNames("table-display-settings", className)}>
            <Button variant="secondary" onClick={handleOpenSheet}>
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
    onApply: (selectedIds: string[], columnOrder: string[], pinnedIds: string[]) => void;
}

function TableDisplaySettingsSheet({
    columnMetas,
    allColumnIds,
    initialSelectedIds,
    initialColumnOrder,
    initialPinnedIds,
    onApply,
}: TableDisplaySettingsSheetProps) {
    const { close } = useViewSheet();

    const [columnOrder, setColumnOrder] = useState<string[]>(initialColumnOrder);
    const [selectedIds, setSelectedIds] = useState<string[]>(initialSelectedIds);
    const [pinnedIds, setPinnedIds] = useState<string[]>(initialPinnedIds);
    const [activeDragId, setActiveDragId] = useState<string>(null);

    const sensors = useSensors(useSensor(PointerSensor));

    const hideableIds = columnMetas.filter((m) => m.canHide).map((m) => m.id);
    const selectionState = genUtils.getSelectionState(
        hideableIds,
        selectedIds.filter((id) => hideableIds.includes(id))
    );

    const metaById = Object.fromEntries(columnMetas.map((m) => [m.id, m]));

    const orderedIds = columnOrder.filter((id) => allColumnIds.includes(id));
    const pinnedColumnIds = orderedIds.filter((id) => pinnedIds.includes(id));
    const unpinnedColumnIds = orderedIds.filter((id) => !pinnedIds.includes(id));

    const handleToggleAll = () => {
        if (selectionState === "Empty") {
            setSelectedIds(columnMetas.map((m) => m.id));
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
        setColumnOrder(initialColumnOrder);
        setSelectedIds(initialSelectedIds);
        setPinnedIds(initialPinnedIds);
    };

    const handleApply = () => {
        onApply(selectedIds, columnOrder, pinnedIds);
        close();
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
                                        {pinnedColumnIds.map((id) => (
                                            <SortableColumnRow
                                                key={id}
                                                id={id}
                                                meta={metaById[id]}
                                                isSelected={selectedIds.includes(id)}
                                                isPinned
                                                isDraggingActive={activeDragId !== null}
                                                onToggle={() => handleToggleOne(id)}
                                                onTogglePin={() => handleTogglePin(id)}
                                            />
                                        ))}
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
                                {unpinnedColumnIds.map((id) => (
                                    <SortableColumnRow
                                        key={id}
                                        id={id}
                                        meta={metaById[id]}
                                        isSelected={selectedIds.includes(id)}
                                        isPinned={false}
                                        isDraggingActive={activeDragId !== null}
                                        onToggle={() => handleToggleOne(id)}
                                        onTogglePin={() => handleTogglePin(id)}
                                    />
                                ))}
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

interface ColumnRowProps {
    id: string;
    meta: ColumnMeta;
    isSelected: boolean;
    isPinned: boolean;
    onToggle?: () => void;
    onTogglePin?: () => void;
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

function ColumnRowPreview({ id, meta, isSelected, isPinned }: ColumnRowProps) {
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
