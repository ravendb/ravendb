import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import Table from "react-bootstrap/Table";
import performanceHint from "common/notifications/models/performanceHint";
import genUtils from "common/generalUtils";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { dateColumnOptions, previewColumn } from "../NotificationDetailsCells";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";
import { NotificationDetailsPreview } from "../NotificationDetailsPreview";
import { usePreviewItem } from "../usePreviewItem";

type LoadedReference =
    Raven.Server.NotificationCenter.Notifications.Details.IndexingReferenceLoadWarning.LoadedReference;

interface IndexReferencesWarning {
    indexName: string;
    lastWarningTime: string;
    loadedReferences: LoadedReference[];
}

const dataColumns: ColumnDef<IndexReferencesWarning>[] = [
    {
        header: "Index Name",
        accessorKey: "indexName",
        meta: { relativeWidth: 60 },
    },
    {
        header: "Last Warning Date",
        accessorKey: "lastWarningTime",
        ...dateColumnOptions,
    },
];

export default function IndexingReferencesDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"Indexing_References">>) {
    const details = useKnockoutValue(notification.details);

    const warnings = useMemo(
        () =>
            Object.entries(details.Warnings ?? {})
                .map(([indexName, warning]) => ({
                    indexName,
                    lastWarningTime: warning.LastWarningTime,
                    loadedReferences: Object.values(warning.Top10LoadedReferences ?? {}),
                }))
                .reverse(),
        [details]
    );

    const [selectedWarning, setSelectedWarning] = usePreviewItem(warnings, getWarningKey);

    const columns = useMemo(
        () => [previewColumn<IndexReferencesWarning>(setSelectedWarning), ...dataColumns],
        [setSelectedWarning]
    );

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={warnings}
                columns={columns}
                itemName="index"
                itemNamePlural="indexes"
                selectedItem={selectedWarning}
                onItemClick={setSelectedWarning}
            />
            {selectedWarning && (
                <NotificationDetailsPreview title="Top loaded references" item={selectedWarning}>
                    <NotificationDetailsList>
                        <NotificationDetailsListItem label="Index Name">
                            {selectedWarning.indexName}
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label="Last warning time">
                            {genUtils.formatUtcDateAsLocal(selectedWarning.lastWarningTime)}
                        </NotificationDetailsListItem>
                    </NotificationDetailsList>
                    <Table striped size="sm" className="mb-0">
                        <thead>
                            <tr>
                                <th>Reference ID</th>
                                <th>Number of loads</th>
                            </tr>
                        </thead>
                        <tbody>
                            {selectedWarning.loadedReferences.map((reference, index) => (
                                <tr key={index}>
                                    <td>{reference.ReferenceId}</td>
                                    <td>{reference.NumberOfLoads.toLocaleString()}</td>
                                </tr>
                            ))}
                        </tbody>
                    </Table>
                </NotificationDetailsPreview>
            )}
        </PerformanceHintDetailsModal>
    );
}

function getWarningKey(warning: IndexReferencesWarning): string {
    return warning.indexName;
}
