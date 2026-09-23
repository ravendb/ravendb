import "./VirtualTable.scss";
import { ClassNameProps } from "components/models/common";
import VirtualTableBodyWrapper from "./partials/VirtualTableBodyWrapper";
import LazyVirtualTableRow from "./partials/LazyVirtualTableRow";
import LazyVirtualTablePaginationBar from "./partials/LazyVirtualTablePaginationBar";
import { LazyRows } from "./hooks/useLazyRows";
import { useLazyTableViewport } from "./hooks/useLazyTableViewport";
import { useSelectionPreviewRange } from "./hooks/useSelectionPreviewRange";
import { isInRange } from "./utils/lazyTableUtils";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { ReactNode, useState } from "react";
import { Table as TanstackTable } from "@tanstack/react-table";

export interface LazyVirtualTableProps<T> extends ClassNameProps {
    table: TanstackTable<T>;
    lazyRows: LazyRows<T>;
    // controlled when provided
    isPaginated?: boolean;
    onIsPaginatedChange?: (isPaginated: boolean) => void;
    // fixed height instead of the measured one
    heightInPx?: number;
    isCompact?: boolean;
    overscan?: number;
    emptyMessage?: ReactNode;
    itemsName?: string;
    bottomOverlay?: ReactNode;
    isRoundingDisabled?: boolean;
    isPaddingDisabled?: boolean;
}

export default function LazyVirtualTable<T>({
    table,
    lazyRows,
    isPaginated: controlledIsPaginated,
    onIsPaginatedChange,
    heightInPx,
    isCompact = false,
    overscan = 20,
    className,
    emptyMessage,
    itemsName = "items",
    bottomOverlay,
    isRoundingDisabled,
    isPaddingDisabled,
}: LazyVirtualTableProps<T>) {
    const [uncontrolledIsPaginated, setUncontrolledIsPaginated] = useState(false);
    const isPaginated = controlledIsPaginated ?? uncontrolledIsPaginated;

    const setIsPaginated = (value: boolean) => {
        setUncontrolledIsPaginated(value);
        onIsPaginatedChange?.(value);
    };

    const viewport = useLazyTableViewport({
        lazyRows,
        isPaginated,
        setIsPaginated,
        fixedHeightInPx: heightInPx,
        isCompact,
        overscan,
    });

    const selectionPreviewRange = useSelectionPreviewRange({
        anchorRowIndex: table.options.meta?.lazySelection?.anchorRowIndex ?? null,
        containerRef: viewport.containerRef,
    });

    const { columnSizing, columnVisibility, columnOrder, columnPinning } = table.getState();
    const cellsRenderDependencies = [table.options.columns, columnSizing, columnVisibility, columnOrder, columnPinning];

    return (
        <div className="lazy-virtual-table">
            <div ref={viewport.areaRef} className="lazy-virtual-table-area">
                <VirtualTableBodyWrapper
                    table={table}
                    className={className}
                    tableContainerRef={viewport.containerRef}
                    isLoading={viewport.isLoading}
                    isEmpty={viewport.isEmpty}
                    emptyMessage={emptyMessage}
                    heightInPx={viewport.heightInPx}
                    isCompact={isCompact}
                    isRoundingDisabled={isRoundingDisabled}
                    isPaddingDisabled={isPaddingDisabled}
                    overlay={
                        <>
                            {(viewport.isDomLimitBannerVisible || bottomOverlay) && (
                                <div className="floating-bars-container">
                                    {viewport.isDomLimitBannerVisible && (
                                        <div className="floating-bar text-nowrap" data-testid="dom-limit-banner">
                                            <span>
                                                It looks like you&apos;ve reached the end of the DOM.{" "}
                                                <Button
                                                    variant="link"
                                                    className="p-0 align-baseline"
                                                    onClick={viewport.turnOnPagination}
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
                            {viewport.isScrollToTopVisible && (
                                <Button
                                    variant="secondary"
                                    className="floating-bar scroll-to-top rounded-pill"
                                    title="Scroll to top"
                                    aria-label="Scroll to top"
                                    onClick={viewport.scrollToTop}
                                >
                                    <Icon icon="arrow-thin-top" margin="m-0" />
                                </Button>
                            )}
                        </>
                    }
                >
                    <tbody style={{ height: viewport.bodyHeightInPx }}>
                        {table.getRowModel().rows.map((row) => {
                            const rowIndex = lazyRows.rows[row.index].index;

                            return (
                                <LazyVirtualTableRow
                                    key={rowIndex - viewport.firstRowIndex}
                                    row={row}
                                    rowIndex={rowIndex}
                                    firstRowIndex={viewport.firstRowIndex}
                                    rowHeightInPx={viewport.rowHeightInPx}
                                    isCompact={isCompact}
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
            {viewport.pagination && <LazyVirtualTablePaginationBar pagination={viewport.pagination} />}
        </div>
    );
}
