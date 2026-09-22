import { PropsWithChildren, ReactNode } from "react";
import { virtualTableUtils } from "../utils/virtualTableUtils";
import VirtualTableHead from "./VirtualTableHead";
import { VirtualTableState } from "./VirtualTableState";
import classNames from "classnames";
import Table from "react-bootstrap/Table";
import { Table as TanstackTable } from "@tanstack/react-table";
import { ClassNameProps } from "../../../models/common";

export interface VirtualTableBodyWrapperProps<T> {
    table: TanstackTable<T>;
    heightInPx: number;
    isLoading?: boolean;
    // overrides the default "no rows in the table" check, e.g. when rows are fetched lazily
    isEmpty?: boolean;
    emptyMessage?: ReactNode;
    tableContainerRef: React.MutableRefObject<HTMLDivElement>;
    isCompact?: boolean;
    isRoundingDisabled?: boolean;
    isPaddingDisabled?: boolean;
    // rendered on top of the scrollable area (e.g. a banner)
    overlay?: ReactNode;
}

export default function VirtualTableBodyWrapper<T>({
    table,
    className,
    tableContainerRef,
    isLoading,
    isEmpty,
    emptyMessage,
    heightInPx,
    isCompact,
    isRoundingDisabled,
    isPaddingDisabled,
    overlay,
    children,
}: PropsWithChildren<VirtualTableBodyWrapperProps<T>> & ClassNameProps) {
    const tableHeightInPx = virtualTableUtils.getTableContainerHeightInPx(heightInPx, isPaddingDisabled);

    return (
        <div
            className={classNames(
                "virtual-table",
                { "p-0": isPaddingDisabled },
                { "rounded-0": isRoundingDisabled },
                className
            )}
        >
            <VirtualTableState
                isLoading={isLoading}
                isEmpty={isEmpty ?? table.getRowCount() === 0}
                emptyMessage={emptyMessage}
            />

            <div className="position-relative">
                <div
                    ref={tableContainerRef}
                    className={classNames("table-container", { "rounded-0": isRoundingDisabled })}
                    style={{ height: tableHeightInPx }}
                >
                    <Table className="m-0" borderless>
                        <VirtualTableHead table={table} isCompact={isCompact} />
                        {children}
                    </Table>
                </div>
                {overlay}
            </div>
        </div>
    );
}
