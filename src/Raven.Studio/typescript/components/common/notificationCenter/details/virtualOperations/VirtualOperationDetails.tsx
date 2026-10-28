import { ColumnDef } from "@tanstack/react-table";
import groupedVirtualNotification from "common/notifications/models/groupedVirtualNotification";
import virtualBulkInsert from "common/notifications/models/virtualBulkInsert";
import virtualBulkInsertFailures from "common/notifications/models/virtualBulkInsertFailures";
import virtualDeleteByQuery from "common/notifications/models/virtualDeleteByQuery";
import virtualDeleteByQueryFailures from "common/notifications/models/virtualDeleteByQueryFailures";
import virtualUpdateByQuery from "common/notifications/models/virtualUpdateByQuery";
import virtualUpdateByQueryFailures from "common/notifications/models/virtualUpdateByQueryFailures";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { NotificationDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CloseDetailsFooter } from "../NotificationDetailsFooters";
import { columnSizes, dateColumnOptions } from "../NotificationDetailsCells";

type OperationItem = { id: string; operationId?: number };

interface VirtualOperationDetailsProps<T extends OperationItem>
    extends NotificationDetailsProps<groupedVirtualNotification<T>> {
    title: string;
    columns: ColumnDef<T>[];
}

function VirtualOperationDetails<T extends OperationItem>({
    notification,
    actions,
    title,
    columns,
}: VirtualOperationDetailsProps<T>) {
    const operations = useKnockoutValue(notification.operations);

    return (
        <NotificationDetailsModal
            notification={notification}
            onClose={actions.close}
            title={title}
            isMessageHidden
            size="xxl"
            footer={<CloseDetailsFooter actions={actions} />}
        >
            <NotificationDetailsTable
                notification={notification}
                data={operations}
                columns={columns}
                itemName="operation"
                itemNamePlural="operations"
                exportTitle={title}
            />
        </NotificationDetailsModal>
    );
}

function dateColumn<T extends { date: string }>(): ColumnDef<T> {
    return {
        id: "date",
        header: "Date",
        accessorFn: (x) => x.date,
        ...dateColumnOptions,
    };
}

function durationColumn<T extends { duration: number }>(): ColumnDef<T> {
    return {
        id: "duration",
        header: "Duration (ms)",
        accessorFn: (x) => x.duration,
        size: columnSizes.duration,
    };
}

const bulkInsertColumns: ColumnDef<virtualBulkOperationItem>[] = [
    dateColumn(),
    durationColumn(),
    { header: "Documents", accessorKey: "documentsProcessed", meta: { relativeWidth: 17 } },
    { header: "Attachments", accessorKey: "attachmentsProcessed", meta: { relativeWidth: 17 } },
    { header: "Counters", accessorKey: "countersProcessed", meta: { relativeWidth: 17 } },
    { header: "Time Series", accessorKey: "timeSeriesProcessed", meta: { relativeWidth: 17 } },
];

const bulkInsertFailureColumns: ColumnDef<virtualBulkOperationFailureItem>[] = [
    dateColumn(),
    durationColumn(),
    { header: "Error message", accessorKey: "errorMsg", meta: { relativeWidth: 25 } },
    { header: "Error", accessorKey: "error", meta: { relativeWidth: 43 } },
];

const queryBasedColumns: ColumnDef<queryBasedVirtualBulkOperationItem>[] = [
    dateColumn(),
    durationColumn(),
    { header: "Processed documents", accessorKey: "totalItemsProcessed", meta: { relativeWidth: 15 } },
    { header: "Collection/Index", accessorKey: "indexOrCollectionUsed", meta: { relativeWidth: 20 } },
    { header: "Query", accessorKey: "query", meta: { relativeWidth: 33 } },
];

const queryBasedFailureColumns: ColumnDef<queryBasedVirtualBulkOperationFailureItem>[] = [
    dateColumn(),
    durationColumn(),
    { header: "Query", accessorKey: "query", meta: { relativeWidth: 25 } },
    { header: "Error message", accessorKey: "errorMsg", meta: { relativeWidth: 18 } },
    { header: "Error", accessorKey: "error", meta: { relativeWidth: 25 } },
];

export function VirtualBulkInsertDetails(props: NotificationDetailsProps<virtualBulkInsert>) {
    return <VirtualOperationDetails {...props} title="Bulk inserts - inserted items" columns={bulkInsertColumns} />;
}

export function VirtualBulkInsertFailuresDetails(props: NotificationDetailsProps<virtualBulkInsertFailures>) {
    return <VirtualOperationDetails {...props} title="Bulk inserts - failures" columns={bulkInsertFailureColumns} />;
}

export function VirtualUpdateByQueryDetails(props: NotificationDetailsProps<virtualUpdateByQuery>) {
    return <VirtualOperationDetails {...props} title="Update by query" columns={queryBasedColumns} />;
}

export function VirtualUpdateByQueryFailuresDetails(props: NotificationDetailsProps<virtualUpdateByQueryFailures>) {
    return <VirtualOperationDetails {...props} title="Update by query - failures" columns={queryBasedFailureColumns} />;
}

export function VirtualDeleteByQueryDetails(props: NotificationDetailsProps<virtualDeleteByQuery>) {
    return <VirtualOperationDetails {...props} title="Delete by query" columns={queryBasedColumns} />;
}

export function VirtualDeleteByQueryFailuresDetails(props: NotificationDetailsProps<virtualDeleteByQueryFailures>) {
    return <VirtualOperationDetails {...props} title="Delete by query - failures" columns={queryBasedFailureColumns} />;
}
