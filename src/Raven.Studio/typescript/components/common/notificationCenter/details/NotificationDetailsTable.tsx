import { useMemo } from "react";
import classNames from "classnames";
import {
    ColumnDef,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    useReactTable,
} from "@tanstack/react-table";
import Button from "react-bootstrap/Button";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import SizeGetter from "components/common/SizeGetter";
import { Icon } from "components/common/Icon";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import fileDownloader from "common/fileDownloader";
import copyToClipboard from "common/copyToClipboard";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import abstractNotification from "common/notifications/models/abstractNotification";
import { CellDetailsValue } from "./NotificationDetailsCells";
import { getCsvFileName, tableToCsv } from "./notificationDetailsCsv";

interface NotificationDetailsTableProps<T> {
    notification: abstractNotification;
    data: T[];
    columns: ColumnDef<T>[];
    itemName: string;
    itemNamePlural: string;
    exportTitle?: string;
    selectedItem?: T;
    onItemClick?: (item: NoInfer<T>) => void;
}

const maxTableHeight = "50vh";

export function NotificationDetailsTable<T>(props: NotificationDetailsTableProps<T>) {
    return (
        <SizeGetter
            className="vstack gap-1 flex-shrink-0"
            style={{ height: "auto" }}
            render={({ width }) => <SizedNotificationDetailsTable {...props} width={width} />}
        />
    );
}

function SizedNotificationDetailsTable<T>({
    notification,
    data,
    columns,
    itemName,
    itemNamePlural,
    exportTitle,
    selectedItem,
    onItemClick,
    width,
}: NotificationDetailsTableProps<T> & { width: number }) {
    const fittedColumns = useMemo(() => virtualTableUtils.fitColumnsToWidth(columns, width), [columns, width]);

    const table = useReactTable<T>({
        data,
        columns: fittedColumns,
        defaultColumn: {
            cell: CellDetailsValue,
        },
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    const visibleCount = table.getRowModel().rows.length;
    const fullHeightInPx = virtualTableUtils.getHeightInPx(visibleCount, Infinity);
    const minHeightInPx = virtualTableUtils.getHeightInPx(Math.min(visibleCount, 3), Infinity);

    const exportToCsv = () => {
        const fileName = getCsvFileName(notification.databaseName, exportTitle ?? notification.title());
        fileDownloader.downloadAsCsv(tableToCsv(table), fileName);
    };

    const copyAsCsv = () => {
        copyToClipboard.copy(tableToCsv(table), "Table has been copied to clipboard as CSV");
    };

    return (
        <>
            <div className="hstack justify-content-between">
                <span className="text-muted small">
                    {getCountText(visibleCount, data.length, itemName, itemNamePlural)}
                </span>
                <div className="hstack gap-1">
                    <Button
                        variant="secondary"
                        size="sm"
                        className="lh-base"
                        onClick={copyAsCsv}
                        disabled={visibleCount === 0}
                    >
                        <Icon icon="copy-to-clipboard" />
                        Copy as CSV
                    </Button>
                    <Button
                        variant="secondary"
                        size="sm"
                        className="lh-base"
                        onClick={exportToCsv}
                        disabled={visibleCount === 0}
                    >
                        <Icon icon="csv-export" />
                        Export to CSV
                    </Button>
                </div>
            </div>
            <SizeGetter
                isHeighRequired
                style={{ height: `min(${fullHeightInPx}px, max(${minHeightInPx}px, ${maxTableHeight}))` }}
                render={({ height }) => (
                    <VirtualTable
                        table={table}
                        heightInPx={height}
                        onRowClick={onItemClick ? (row) => onItemClick(row.original) : undefined}
                        getRowClassName={(row) =>
                            classNames({ "is-selected": row.original === selectedItem, "cursor-pointer": onItemClick })
                        }
                    />
                )}
            />
        </>
    );
}

function getCountText(visibleCount: number, totalCount: number, itemName: string, itemNamePlural: string): string {
    const total = pluralizeHelpers.pluralize(totalCount, itemName, itemNamePlural);
    return visibleCount === totalCount ? total : `${visibleCount.toLocaleString()} of ${total}`;
}
