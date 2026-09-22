import "./VirtualTable.scss";
import { ClassNameProps } from "components/models/common";
import VirtualTableBodyWrapper from "./partials/VirtualTableBodyWrapper";
import LazyVirtualTableRow from "./partials/LazyVirtualTableRow";
import LazyVirtualTablePaginationBar from "./partials/LazyVirtualTablePaginationBar";
import { LazyVirtualTable as LazyVirtualTableModel } from "./hooks/useLazyVirtualTable";
import { useSelectionPreviewRange } from "./hooks/useSelectionPreviewRange";
import { isInRange } from "./utils/lazyTableUtils";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { ReactNode } from "react";
import { Table as TanstackTable } from "@tanstack/react-table";

export interface LazyVirtualTableProps<T> extends ClassNameProps {
    lazyTable: LazyVirtualTableModel<T>;
    table: TanstackTable<T>;
    emptyMessage?: ReactNode;
    itemsName?: string;
    selectionAnchorRowIndex?: number | null;
    bottomOverlay?: ReactNode;
    isRoundingDisabled?: boolean;
    isPaddingDisabled?: boolean;
}

export default function LazyVirtualTable<T>({
    lazyTable,
    table,
    className,
    emptyMessage,
    itemsName = "items",
    selectionAnchorRowIndex = null,
    bottomOverlay,
    isRoundingDisabled,
    isPaddingDisabled,
}: LazyVirtualTableProps<T>) {
    const { rows, view } = lazyTable;

    applyLazyTableOptions(table);

    const selectionPreviewRange = useSelectionPreviewRange({
        anchorRowIndex: selectionAnchorRowIndex,
        containerRef: view.containerRef,
    });

    const { columnSizing, columnVisibility, columnOrder, columnPinning } = table.getState();
    const cellsRenderDependencies = [table.options.columns, columnSizing, columnVisibility, columnOrder, columnPinning];

    return (
        <div className="lazy-virtual-table">
            <div ref={view.areaRef} className="lazy-virtual-table-area">
                <VirtualTableBodyWrapper
                    table={table}
                    className={className}
                    tableContainerRef={view.containerRef}
                    isLoading={view.isLoading}
                    isEmpty={view.isEmpty}
                    emptyMessage={emptyMessage}
                    heightInPx={view.heightInPx}
                    isCompact={view.isCompact}
                    isRoundingDisabled={isRoundingDisabled}
                    isPaddingDisabled={isPaddingDisabled}
                    overlay={
                        <>
                            {(view.isDomLimitBannerVisible || bottomOverlay) && (
                                <div className="floating-bars-container">
                                    {view.isDomLimitBannerVisible && (
                                        <div className="floating-bar text-nowrap" data-testid="dom-limit-banner">
                                            <span>
                                                It looks like you&apos;ve reached the end of the DOM.{" "}
                                                <Button
                                                    variant="link"
                                                    className="p-0 align-baseline"
                                                    onClick={view.onTurnOnPagination}
                                                >
                                                    Turn on pagination
                                                </Button>{" "}
                                                to fetch more {itemsName}
                                            </span>
                                        </div>
                                    )}
                                    {bottomOverlay}
                                </div>
                            )}
                            {view.isScrollToTopVisible && (
                                <Button
                                    variant="secondary"
                                    className="floating-bar scroll-to-top rounded-pill"
                                    title="Scroll to top"
                                    aria-label="Scroll to top"
                                    onClick={view.onScrollToTop}
                                >
                                    <Icon icon="arrow-thin-top" margin="m-0" />
                                </Button>
                            )}
                        </>
                    }
                >
                    <tbody style={{ height: view.bodyHeightInPx }}>
                        {table.getRowModel().rows.map((row, i) => {
                            const rowIndex = rows[i].index;

                            return (
                                <LazyVirtualTableRow
                                    key={rowIndex - view.firstRowIndex}
                                    row={row}
                                    rowIndex={rowIndex}
                                    firstRowIndex={view.firstRowIndex}
                                    rowHeightInPx={view.rowHeightInPx}
                                    isCompact={view.isCompact}
                                    isSelected={row.getIsSelected()}
                                    isSelectionPreview={
                                        !!selectionPreviewRange && isInRange(selectionPreviewRange, rowIndex)
                                    }
                                    renderDependencies={cellsRenderDependencies}
                                />
                            );
                        })}
                    </tbody>
                </VirtualTableBodyWrapper>
            </div>
            {view.pagination && <LazyVirtualTablePaginationBar pagination={view.pagination} />}
        </div>
    );
}

// autoResetPageIndex has to be off so tanstack does not reset its (unused) page index on every fetch
function applyLazyTableOptions<T>(table: TanstackTable<T>) {
    table.setOptions((prev) => ({
        ...prev,
        autoResetPageIndex: false,
        columnResizeMode: "onChange",
        defaultColumn: {
            ...prev.defaultColumn,
            enableSorting: false,
            enableColumnFilter: false,
        },
    }));
}
