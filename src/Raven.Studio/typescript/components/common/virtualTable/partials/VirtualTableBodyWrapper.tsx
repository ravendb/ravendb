import { PropsWithChildren } from "react";
import { virtualTableConstants } from "../utils/virtualTableConstants";
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
    tableContainerRef: React.MutableRefObject<HTMLDivElement>;
    isCompact?: boolean;
    isRoundingDisabled?: boolean;
    isPaddingDisabled?: boolean;
}

export default function VirtualTableBodyWrapper<T>({
    table,
    className,
    tableContainerRef,
    isLoading,
    heightInPx,
    isCompact,
    isRoundingDisabled,
    isPaddingDisabled,
    children,
}: PropsWithChildren<VirtualTableBodyWrapperProps<T>> & ClassNameProps) {
    const paddingInPx = isPaddingDisabled ? 0 : virtualTableConstants.paddingInPx;
    const tableHeightInPx = heightInPx - paddingInPx;

    return (
        <div
            className={classNames(
                "virtual-table",
                { "p-0": isPaddingDisabled },
                { "rounded-0": isRoundingDisabled },
                className
            )}
        >
            <VirtualTableState isLoading={isLoading} isEmpty={table.getRowCount() === 0} />

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
        </div>
    );
}
