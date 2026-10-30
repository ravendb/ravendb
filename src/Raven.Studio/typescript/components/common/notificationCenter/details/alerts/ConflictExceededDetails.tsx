import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions } from "../NotificationDetailsCells";

type ActionDetails = Raven.Server.NotificationCenter.Notifications.Details.ConflictPerformanceDetails.ActionDetails;

const columns: ColumnDef<ActionDetails>[] = [
    {
        header: "Id",
        accessorKey: "Id",
        meta: { relativeWidth: 30 },
    },
    {
        header: "Exceeded Value",
        accessorKey: "Reason",
        meta: { relativeWidth: 30 },
    },
    {
        header: "Deleted Revisions",
        accessorKey: "Deleted",
        meta: { relativeWidth: 20 },
    },
    {
        header: "Date",
        accessorKey: "Time",
        ...dateColumnOptions,
    },
];

export default function ConflictExceededDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"ConflictRevisionsExceeded">>) {
    const details = useKnockoutValue(notification.details);

    const documents = useMemo(() => [...(details.Details ?? [])].reverse(), [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={documents}
                columns={columns}
                itemName="document"
                itemNamePlural="documents"
            />
        </AlertDetailsModal>
    );
}
