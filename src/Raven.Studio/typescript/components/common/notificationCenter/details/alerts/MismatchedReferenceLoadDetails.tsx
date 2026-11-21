import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";

type WarningDetails =
    Raven.Server.NotificationCenter.Notifications.Details.MismatchedReferencesLoadWarning.WarningDetails;

const columns: ColumnDef<WarningDetails>[] = [
    {
        header: "Source ID",
        accessorKey: "SourceId",
        meta: { relativeWidth: 25 },
    },
    {
        header: "Reference ID",
        accessorKey: "ReferenceId",
        meta: { relativeWidth: 25 },
    },
    {
        header: "Actual Collection",
        accessorKey: "ActualCollection",
        meta: { relativeWidth: 25 },
    },
    {
        id: "MismatchedCollections",
        header: "Mismatched collections",
        accessorFn: (x) => x.MismatchedCollections.join(", "),
        meta: { relativeWidth: 25 },
    },
];

export default function MismatchedReferenceLoadDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"MismatchedReferenceLoad">>) {
    const details = useKnockoutValue(notification.details);

    const warnings = useMemo(() => Object.values(details.Warnings ?? {}).flat(), [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={warnings}
                columns={columns}
                itemName="reference"
                itemNamePlural="references"
            />
        </AlertDetailsModal>
    );
}
