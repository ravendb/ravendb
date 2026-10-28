import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import Alert from "react-bootstrap/Alert";
import performanceHint from "common/notifications/models/performanceHint";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions } from "../NotificationDetailsCells";

type WarningDetails = Raven.Server.NotificationCenter.Notifications.Details.WarnIndexOutputsPerDocument.WarningDetails;

interface IndexingWarning extends WarningDetails {
    IndexName: string;
}

const columns: ColumnDef<IndexingWarning>[] = [
    {
        header: "Index Name",
        accessorKey: "IndexName",
        meta: { relativeWidth: 20 },
    },
    {
        header: "# of Exceeding Documents",
        accessorKey: "NumberOfExceedingDocuments",
        meta: { relativeWidth: 20 },
    },
    {
        header: "Sample Document ID",
        accessorKey: "SampleDocumentId",
        meta: { relativeWidth: 20 },
    },
    {
        header: "# of Outputs per Document",
        accessorKey: "MaxNumberOutputsPerDocument",
        meta: { relativeWidth: 20 },
    },
    {
        header: "Date",
        accessorKey: "LastWarningTime",
        ...dateColumnOptions,
    },
];

export default function IndexingDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"Indexing">>) {
    const details = useKnockoutValue(notification.details);

    const warnings = useMemo(
        () =>
            Object.entries(details.Warnings ?? {})
                .flatMap(([indexName, indexWarnings]) => indexWarnings.map((x) => ({ ...x, IndexName: indexName })))
                .reverse(),
        [details]
    );

    const suggestions = useMemo(() => [...new Set(warnings.map((x) => x.Suggestion))].filter(Boolean), [warnings]);

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={warnings}
                columns={columns}
                itemName="warning"
                itemNamePlural="warnings"
            />
            {suggestions.length > 0 && (
                <Alert variant="info" className="mb-0">
                    <strong>{suggestions.length === 1 ? "Suggestion:" : "Suggestions:"}</strong>
                    {suggestions.map((suggestion) => (
                        <div key={suggestion}>{suggestion}</div>
                    ))}
                </Alert>
            )}
        </PerformanceHintDetailsModal>
    );
}
