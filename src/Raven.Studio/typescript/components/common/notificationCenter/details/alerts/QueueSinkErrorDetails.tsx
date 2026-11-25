import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions } from "../NotificationDetailsCells";

type QueueSinkErrorInfo = Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo;

const columns: ColumnDef<QueueSinkErrorInfo>[] = [
    {
        header: "Date",
        accessorKey: "Date",
        ...dateColumnOptions,
    },
    {
        header: "Error",
        accessorKey: "Error",
        meta: { relativeWidth: 75 },
    },
];

export default function QueueSinkErrorDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"QueueSink_ConsumeError" | "QueueSink_ScriptError">>) {
    const details = useKnockoutValue(notification.details);
    const errors = useMemo(() => details.Errors ?? [], [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={errors}
                columns={columns}
                itemName="error"
                itemNamePlural="errors"
            />
        </AlertDetailsModal>
    );
}
