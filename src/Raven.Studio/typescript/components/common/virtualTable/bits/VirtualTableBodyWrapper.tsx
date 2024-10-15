import { PropsWithChildren } from "react";
import { virtualTableConstants } from "../utils/virtualTableConstants";
import VirtualTableHead from "./VirtualTableHead";
import { VirtualTableState } from "./VirtualTableState";
import classNames from "classnames";
import { Table } from "reactstrap";
import { Table as TanstackTable } from "@tanstack/react-table";
import { ClassNameProps } from "../../../models/common";

export interface VirtualTableBodyWrapperProps<T> {
    table: TanstackTable<T>;
    heightInPx: number;
    isLoading?: boolean;
    tableContainerRef: React.MutableRefObject<HTMLDivElement>;
}

export default function VirtualTableBodyWrapper<T>({
    table,
    className,
    tableContainerRef,
    isLoading,
    heightInPx,
    children,
}: PropsWithChildren<VirtualTableBodyWrapperProps<T>> & ClassNameProps) {
    const tableHeightInPx = heightInPx - virtualTableConstants.paddingInPx;

    return (
        <div className={classNames("virtual-table", className)}>
            <VirtualTableState isLoading={isLoading} isEmpty={table.getRowCount() === 0} />

            <div ref={tableContainerRef} className="table-container" style={{ height: tableHeightInPx }}>
                <Table className="m-0" borderless>
                    <VirtualTableHead table={table} />
                    {children}
                </Table>
            </div>
        </div>
    );
}
