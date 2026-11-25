import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";

interface ComplexFieldsItem {
    indexName: string;
    complexFields: string;
}

const columns: ColumnDef<ComplexFieldsItem>[] = [
    {
        header: "Index Name",
        accessorKey: "indexName",
        meta: { relativeWidth: 30 },
    },
    {
        header: "Fields",
        accessorKey: "complexFields",
        meta: { relativeWidth: 70 },
    },
];

export default function ComplexFieldsAlertDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"Indexing_CoraxComplexItem">>) {
    const details = useKnockoutValue(notification.details);

    const items = useMemo(
        () =>
            Object.entries(details.Fields ?? {}).map(([indexName, fields]) => ({
                indexName,
                complexFields: fields.join(", "),
            })),
        [details]
    );

    return (
        <AlertDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={items}
                columns={columns}
                itemName="index"
                itemNamePlural="indexes"
            />
        </AlertDetailsModal>
    );
}
