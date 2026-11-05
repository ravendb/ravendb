import { useMemo } from "react";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import genUtils from "common/generalUtils";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CellWithFormattedValue, columnSizes, dateColumnOptions } from "../NotificationDetailsCells";

type RequestLatencyInfo = Raven.Server.NotificationCenter.Notifications.Details.RequestLatencyInfo;

const columns: ColumnDef<RequestLatencyInfo>[] = [
    {
        header: "Action",
        accessorKey: "Action",
        meta: { relativeWidth: 20 },
    },
    {
        header: "Date",
        accessorKey: "Date",
        ...dateColumnOptions,
    },
    {
        header: "Duration",
        accessorKey: "Duration",
        cell: CellDuration,
        size: columnSizes.duration,
        meta: { exportHeader: "Duration (ms)", filterPlaceholder: "In ms, e.g. > 1200" },
    },
    {
        header: "Query",
        accessorKey: "Query",
        meta: { relativeWidth: 45 },
    },
];

export default function RequestLatencyDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"RequestLatency">>) {
    const details = useKnockoutValue(notification.details);

    const requests = useMemo(() => {
        return Object.entries(details.RequestLatencies ?? {})
            .flatMap(([action, infos]) => infos.map((x) => ({ ...x, Action: action })))
            .reverse();
    }, [details]);

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={requests}
                columns={columns}
                itemName="request"
                itemNamePlural="requests"
            />
        </PerformanceHintDetailsModal>
    );
}

function CellDuration({ getValue }: Pick<CellContext<RequestLatencyInfo, unknown>, "getValue">) {
    const durationInMs = getValue() as number;
    return <CellWithFormattedValue value={durationInMs} formattedValue={genUtils.formatTimeSpan(durationInMs, true)} />;
}
