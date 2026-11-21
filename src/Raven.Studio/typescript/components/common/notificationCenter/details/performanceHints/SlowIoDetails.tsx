import { useMemo } from "react";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CellWithFormattedValue, dateColumnOptions } from "../NotificationDetailsCells";

type SlowWriteInfo = Raven.Server.NotificationCenter.Notifications.Details.SlowIoDetails.SlowWriteInfo;

const columns: ColumnDef<SlowWriteInfo>[] = [
    {
        header: "Path",
        accessorKey: "Path",
        meta: { relativeWidth: 35 },
    },
    {
        header: "Type",
        accessorKey: "Type",
        meta: { relativeWidth: 15 },
    },
    {
        header: "Date",
        accessorKey: "Date",
        ...dateColumnOptions,
    },
    {
        header: "Speed",
        accessorKey: "SpeedInMbPerSec",
        cell: CellSpeed,
        meta: { relativeWidth: 30, exportHeader: "Speed (MB/s)" },
    },
];

export default function SlowIoDetails({ notification, actions }: NotificationDetailsProps<performanceHint<"SlowIO">>) {
    const details = useKnockoutValue(notification.details);

    const writes = useMemo(() => Object.values(details.Writes ?? {}).reverse(), [details]);

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={writes}
                columns={columns}
                itemName="write"
                itemNamePlural="writes"
            />
        </PerformanceHintDetailsModal>
    );
}

function CellSpeed({ row }: Pick<CellContext<SlowWriteInfo, unknown>, "row">) {
    const { DataWrittenInMb, DurationInSec, SpeedInMbPerSec } = row.original;
    const formattedSpeed = `${DataWrittenInMb.toFixed(2)} MB in ${DurationInSec.toFixed(2)} sec. (${SpeedInMbPerSec.toFixed(2)} MB/s)`;

    return <CellWithFormattedValue value={SpeedInMbPerSec} formattedValue={formattedSpeed} />;
}
