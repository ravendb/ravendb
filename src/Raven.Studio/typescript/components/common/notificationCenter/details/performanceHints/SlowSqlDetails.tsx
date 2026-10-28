import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import performanceHint from "common/notifications/models/performanceHint";
import genUtils from "common/generalUtils";
import Code from "components/common/Code";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { PerformanceHintDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { columnSizes, dateColumnOptions, previewColumn } from "../NotificationDetailsCells";
import { NotificationDetailsList, NotificationDetailsListItem } from "../NotificationDetailsList";
import { NotificationDetailsPreview } from "../NotificationDetailsPreview";
import { usePreviewItem } from "../usePreviewItem";

type SlowSqlStatementInfo = Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo;

const dataColumns: ColumnDef<SlowSqlStatementInfo>[] = [
    {
        header: "Duration (ms)",
        accessorKey: "Duration",
        size: columnSizes.duration,
    },
    {
        header: "Date",
        accessorKey: "Date",
        ...dateColumnOptions,
    },
    {
        header: "Statement",
        accessorKey: "Statement",
        meta: { relativeWidth: 65 },
    },
];

export default function SlowSqlDetails({
    notification,
    actions,
}: NotificationDetailsProps<performanceHint<"SqlEtl_SlowSql">>) {
    const details = useKnockoutValue(notification.details);

    const statements = useMemo(() => [...(details.Statements ?? [])].reverse(), [details]);

    const [selectedStatement, setSelectedStatement] = usePreviewItem(statements, getStatementKey);

    const columns = useMemo(
        () => [previewColumn<SlowSqlStatementInfo>(setSelectedStatement), ...dataColumns],
        [setSelectedStatement]
    );

    return (
        <PerformanceHintDetailsModal notification={notification} actions={actions}>
            <NotificationDetailsTable
                notification={notification}
                data={statements}
                columns={columns}
                itemName="statement"
                itemNamePlural="statements"
                selectedItem={selectedStatement}
                onItemClick={setSelectedStatement}
            />
            {selectedStatement && (
                <NotificationDetailsPreview title="Details" item={selectedStatement}>
                    <NotificationDetailsList>
                        <NotificationDetailsListItem label="Duration">
                            {selectedStatement.Duration.toLocaleString()} ms
                        </NotificationDetailsListItem>
                        <NotificationDetailsListItem label="Date">
                            {genUtils.formatUtcDateAsLocal(selectedStatement.Date)}
                        </NotificationDetailsListItem>
                    </NotificationDetailsList>
                    <Code
                        code={selectedStatement.Statement}
                        language="sql"
                        whiteSpace="pre-wrap"
                        className="notification-details-code"
                    />
                </NotificationDetailsPreview>
            )}
        </PerformanceHintDetailsModal>
    );
}

function getStatementKey(statement: SlowSqlStatementInfo): string {
    return `${statement.Date}|${statement.Statement}`;
}
