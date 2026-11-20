import { useMemo } from "react";
import { CellContext, ColumnDef } from "@tanstack/react-table";
import moment from "moment";
import performanceHint from "common/notifications/models/performanceHint";
import generalUtils from "common/generalUtils";
import { meterTypeLabel } from "common/storageIoTerms";
import { formatMs, formatPercent, formatSyncCost } from "common/storageIoFormat";
import { Icon } from "components/common/Icon";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";
import { CellWithFormattedValue, dateColumnOptions } from "../NotificationDetailsCells";

type StorageCongestionDetailsDto = Raven.Server.NotificationCenter.Notifications.Details.StorageCongestionDetails;
type SlowWriteInfo = Raven.Server.NotificationCenter.Notifications.Details.SlowIoDetails.SlowWriteInfo;

const columns: ColumnDef<SlowWriteInfo>[] = [
    {
        header: "Path",
        accessorKey: "Path",
        meta: { relativeWidth: 30 },
    },
    {
        id: "Type",
        header: "Type",
        accessorFn: (x) => meterTypeLabel[x.Type] ?? x.Type,
        meta: { relativeWidth: 12 },
    },
    {
        header: "Size",
        accessorKey: "DataWrittenInMb",
        cell: (ctx) => <CellNumber {...ctx} unit="MB" />,
        meta: { relativeWidth: 11, exportHeader: "Size (MB)" },
    },
    {
        header: "Duration",
        accessorKey: "DurationInSec",
        cell: (ctx) => <CellNumber {...ctx} unit="s" />,
        meta: { relativeWidth: 11, exportHeader: "Duration (s)" },
    },
    {
        header: "Speed",
        accessorKey: "SpeedInMbPerSec",
        cell: (ctx) => <CellNumber {...ctx} unit="MB/s" />,
        meta: { relativeWidth: 13, exportHeader: "Speed (MB/s)" },
    },
    {
        header: "Date",
        accessorKey: "Date",
        ...dateColumnOptions,
    },
];

export default function StorageCongestionDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"StorageCongestion">>) {
    const details = useKnockoutValue(notification.details);

    // worst first
    const writes = useMemo(
        () => Object.values(details.Writes ?? {}).sort((a, b) => b.DurationInSec - a.DurationInSec),
        [details]
    );

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsList>
                {getSummaryItems(details).map((item) => (
                    <NotificationDetailsListItem key={item.name} label={item.name}>
                        {item.value}
                    </NotificationDetailsListItem>
                ))}
            </NotificationDetailsList>
            {!details.SlowWritesMeasured ? (
                <div className="text-muted">
                    <Icon icon="info" />
                    Slow writes are not measured: Storage.IO.Metrics.Enabled is off
                </div>
            ) : writes.length === 0 ? (
                <div className="text-muted">
                    <Icon icon="info" />
                    No slow writes on this disk in the last hour.
                </div>
            ) : (
                <>
                    <h4 className="mb-0">Slowest writes on this disk in the last hour</h4>
                    <NotificationDetailsTable
                        notification={notification}
                        data={writes}
                        columns={columns}
                        itemName="write"
                        itemNamePlural="writes"
                    />
                </>
            )}
        </PerformanceHintDetailsModal>
    );
}

function CellNumber({ getValue, unit }: Pick<CellContext<SlowWriteInfo, unknown>, "getValue"> & { unit: string }) {
    const value = getValue<number>();
    return <CellWithFormattedValue value={value} formattedValue={`${formatNumber(value)} ${unit}`} />;
}

function getSummaryItems(details: StorageCongestionDetailsDto): { name: string; value: string }[] {
    const items: { name: string; value: string }[] = [];

    if (details.Cause) {
        items.push({ name: "Cause", value: details.Cause.charAt(0).toUpperCase() + details.Cause.slice(1) });
    }

    if (details.SaturatedSince) {
        const since = generalUtils.formatUtcDateAsLocal(details.SaturatedSince);
        const ago = generalUtils.formatDurationByDate(moment.utc(details.SaturatedSince), true);
        items.push({ name: "Saturated since", value: `${since} (${ago})` });
    }

    items.push({ name: "Writes paced in the last hour", value: formatPercent(details.DrainShareOfLastHour) });

    if (details.TypicalJournalWriteLatencyInMs != null) {
        items.push({
            name: "Journal writes in the last hour",
            value: `typically ${formatMs(details.TypicalJournalWriteLatencyInMs)}, peak ${formatMs(details.PeakJournalWriteLatencyInMs)}`,
        });
    }

    items.push(
        { name: "Journal writes now", value: formatMs(details.JournalWriteLatencyInMs) },
        { name: "Data syncs", value: formatSyncCost(details.SyncCostInMs) },
        { name: "Mount point", value: details.MountPoint ?? "-" }
    );

    return items;
}

function formatNumber(value: number): string {
    return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
