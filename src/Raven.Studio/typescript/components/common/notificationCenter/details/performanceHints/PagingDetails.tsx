import { useMemo } from "react";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import genUtils from "common/generalUtils";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CellBytes, CellDetailsValue, columnSizes, dateColumnOptions } from "../NotificationDetailsCells";

type ActionDetails = Raven.Server.NotificationCenter.Notifications.Details.PagingPerformanceDetails.ActionDetails;

interface PagingItem extends ActionDetails {
    Action: string;
}

const pageSizeNotSetText = "Not set";

const columns: ColumnDef<PagingItem>[] = [
    {
        header: "Action",
        accessorKey: "Action",
        meta: { relativeWidth: 14 },
    },
    {
        header: "# of results",
        accessorKey: "NumberOfResults",
        meta: { relativeWidth: 14 },
    },
    {
        id: "PageSize",
        header: "Page size",
        accessorFn: (x) => (x.PageSize === genUtils.integerMaxValue ? pageSizeNotSetText : x.PageSize),
        cell: CellPageSize,
        sortingFn: "alphanumeric",
        meta: { relativeWidth: 12 },
    },
    {
        header: "Docs size",
        accessorKey: "TotalDocumentsSizeInBytes",
        cell: CellBytes,
        size: columnSizes.bytes,
        meta: { exportHeader: "Docs size (bytes)", filterPlaceholder: "e.g. > 5 MB" },
    },
    {
        header: "Date",
        accessorKey: "Occurrence",
        ...dateColumnOptions,
    },
    {
        header: "Duration (ms)",
        accessorKey: "Duration",
        size: columnSizes.duration,
    },
    {
        header: "Details",
        accessorKey: "Details",
        meta: { relativeWidth: 29 },
    },
];

export default function PagingDetails({ notification, actions }: NotificationDetailsProps<performanceHint<"Paging">>) {
    const details = useKnockoutValue(notification.details);

    const items = useMemo(
        () =>
            Object.entries(details.Actions ?? {})
                .flatMap(([action, actionDetails]) => actionDetails.map((x) => ({ ...x, Action: action })))
                .reverse(),
        [details]
    );

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={items}
                columns={columns}
                itemName="request"
                itemNamePlural="requests"
            />
        </PerformanceHintDetailsModal>
    );
}

function CellPageSize({ getValue }: Pick<CellContext<PagingItem, unknown>, "getValue">) {
    if (getValue() === pageSizeNotSetText) {
        return <span>{pageSizeNotSetText}</span>;
    }

    return <CellDetailsValue getValue={getValue} />;
}
