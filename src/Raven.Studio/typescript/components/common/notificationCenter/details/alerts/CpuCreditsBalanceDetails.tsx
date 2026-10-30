import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";

interface PausedIndex {
    indexName: string;
}

const columns: ColumnDef<PausedIndex>[] = [
    {
        header: "Index Name",
        accessorKey: "indexName",
        meta: { relativeWidth: 100 },
    },
];

export default function CpuCreditsBalanceDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"Throttling_CpuCreditsBalance">>) {
    const details = useKnockoutValue(notification.details);

    const pausedIndexes = useMemo(() => (details.IndexNames ?? []).map((indexName) => ({ indexName })), [details]);

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            <h4 className="mb-0">Paused indexes</h4>
            <NotificationDetailsTable
                notification={notification}
                data={pausedIndexes}
                columns={columns}
                itemName="index"
                itemNamePlural="indexes"
            />
        </AlertDetailsModal>
    );
}
