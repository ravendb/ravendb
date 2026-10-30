import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import { RemoteAttachmentErrorInfo } from "common/notifications/models/notificationDetailsByReason";
import genUtils from "common/generalUtils";
import Code from "components/common/Code";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions, previewColumn } from "../NotificationDetailsCells";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";
import { NotificationDetailsPreview } from "../NotificationDetailsPreview";
import { usePreviewItem } from "../usePreviewItem";

const dataColumns: ColumnDef<RemoteAttachmentErrorInfo>[] = [
    {
        header: "Date",
        accessorKey: "Date",
        ...dateColumnOptions,
    },
    {
        id: "Ids",
        header: "Ids",
        accessorFn: (x) => x.Ids?.join(", "),
        meta: { relativeWidth: 15 },
    },
    {
        header: "Identifier",
        accessorKey: "Identifier",
        meta: { relativeWidth: 20 },
    },
    {
        header: "Error",
        accessorKey: "Error",
        meta: { relativeWidth: 50 },
    },
];

export default function RemoteAttachmentErrorDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"Attachments_RemoteAttachmentErroredIdentifier">>) {
    const details = useKnockoutValue(notification.details);

    const errors = useMemo(() => [...(details.Errors ?? [])].reverse(), [details]);

    const [selectedError, setSelectedError] = usePreviewItem(errors, getErrorKey);

    const columns = useMemo(
        () => [previewColumn<RemoteAttachmentErrorInfo>(setSelectedError), ...dataColumns],
        [setSelectedError]
    );

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={errors}
                columns={columns}
                itemName="error"
                itemNamePlural="errors"
                selectedItem={selectedError}
                onItemClick={setSelectedError}
            />
            {selectedError && (
                <NotificationDetailsPreview title="Details" item={selectedError}>
                    <NotificationDetailsList>
                        <NotificationDetailsListItem label="Identifier">
                            {selectedError.Identifier}
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label="Hash">{selectedError.Hash}</NotificationDetailsListItem>
                        <NotificationDetailsListItem label="Document IDs">
                            {selectedError.Ids?.length ? selectedError.Ids.join(", ") : "-"}
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label="Date">
                            {genUtils.formatUtcDateAsLocal(selectedError.Date)}
                        </NotificationDetailsListItem>
                    </NotificationDetailsList>
                    <Code
                        code={selectedError.Error}
                        language="plaintext"
                        whiteSpace="pre-wrap"
                        className="notification-details-code"
                    />
                </NotificationDetailsPreview>
            )}
        </AlertDetailsModal>
    );
}

function getErrorKey(error: RemoteAttachmentErrorInfo): string {
    return `${error.Date}|${error.Identifier}|${error.Hash}`;
}
