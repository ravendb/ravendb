import { PropsWithChildren, ReactNode, useState } from "react";
import { virtualTableUtils } from "../utils/virtualTableUtils";
import VirtualTableHead from "./VirtualTableHead";
import { VirtualTableState } from "./VirtualTableState";
import classNames from "classnames";
import Button from "react-bootstrap/Button";
import Table from "react-bootstrap/Table";
import { Table as TanstackTable } from "@tanstack/react-table";
import { Icon } from "components/common/Icon";
import { ClassNameProps } from "../../../models/common";

export interface VirtualTableBodyWrapperProps<T> {
    table: TanstackTable<T>;
    heightInPx: number;
    isLoading?: boolean;
    isEmpty?: boolean;
    emptyMessage?: ReactNode;
    tableContainerRef: React.MutableRefObject<HTMLDivElement>;
    onScroll?: () => void;
    isCompact?: boolean;
    isRoundingDisabled?: boolean;
    isPaddingDisabled?: boolean;
    overlay?: ReactNode;
}

export default function VirtualTableBodyWrapper<T>({
    table,
    className,
    tableContainerRef,
    onScroll,
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
    const [isScrolledFromTop, setIsScrolledFromTop] = useState(false);

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
                {overlay}
                <div
                    ref={tableContainerRef}
                    className={classNames("table-container", { "rounded-0": isRoundingDisabled })}
                    style={{ height: tableHeightInPx }}
                    onScroll={(e) => {
                        setIsScrolledFromTop(e.currentTarget.scrollTop > 0);
                        onScroll?.();
                    }}
                >
                    <Table className="m-0" borderless>
                        <VirtualTableHead table={table} isCompact={isCompact} />
                        {children}
                    </Table>
                </div>
                {isScrolledFromTop && (
                    <Button
                        variant="secondary"
                        className="scroll-to-top rounded-pill"
                        title="Scroll to top"
                        aria-label="Scroll to top"
                        onClick={() => tableContainerRef.current.scrollTo({ top: 0, behavior: "instant" })}
                    >
                        <Icon icon="arrow-thin-top" margin="m-0" />
                    </Button>
                )}
            </div>
        </div>
    );
}
