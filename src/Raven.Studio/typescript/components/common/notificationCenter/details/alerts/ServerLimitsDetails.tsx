import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions } from "../NotificationDetailsCells";

type ServerLimitInfo = Raven.Server.NotificationCenter.Notifications.Details.ServerLimitsDetails.ServerLimitInfo;

const columns: ColumnDef<ServerLimitInfo>[] = [
    {
        header: "Date",
        accessorKey: "Date",
        ...dateColumnOptions,
    },
    {
        header: "Name",
        accessorKey: "Name",
        meta: { relativeWidth: 25 },
    },
    {
        header: "Limit",
        accessorKey: "Limit",
        meta: { relativeWidth: 20 },
    },
    {
        header: "Value",
        accessorKey: "Value",
        meta: { relativeWidth: 15 },
    },
    {
        header: "Max",
        accessorKey: "Max",
        meta: { relativeWidth: 15 },
    },
];

export default function ServerLimitsDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"ServerLimits">>) {
    const details = useKnockoutValue(notification.details);
    const limits = useMemo(() => details.Limits ?? [], [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={limits}
                columns={columns}
                itemName="limit"
                itemNamePlural="limits"
            />
        </AlertDetailsModal>
    );
}
