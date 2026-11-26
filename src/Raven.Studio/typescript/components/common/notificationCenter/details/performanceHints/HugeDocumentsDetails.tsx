import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CellBytes, columnSizes, dateColumnOptions } from "../NotificationDetailsCells";

type HugeDocumentInfo = Raven.Server.NotificationCenter.Notifications.Details.HugeDocumentInfo;

const columns: ColumnDef<HugeDocumentInfo>[] = [
    {
        header: "Document ID",
        accessorKey: "Id",
        meta: { relativeWidth: 40 },
    },
    {
        header: "Document size",
        accessorKey: "Size",
        cell: CellBytes,
        size: columnSizes.bytes,
        meta: { exportHeader: "Document size (bytes)", filterPlaceholder: "e.g. > 5 MB" },
    },
    {
        header: "Last access",
        accessorKey: "Date",
        ...dateColumnOptions,
    },
];

export default function HugeDocumentsDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"HugeDocuments">>) {
    const details = useKnockoutValue(notification.details);

    const documents = useMemo(() => Object.values(details.HugeDocuments ?? {}).reverse(), [details]);

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={documents}
                columns={columns}
                itemName="document"
                itemNamePlural="documents"
            />
        </PerformanceHintDetailsModal>
    );
}
