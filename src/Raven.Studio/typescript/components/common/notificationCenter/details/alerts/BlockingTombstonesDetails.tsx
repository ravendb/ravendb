import { useMemo } from "react";
import { ColumnDef } from "@tanstack/react-table";
import alert from "common/notifications/models/alert";
import appUrl from "common/appUrl";
import { useKnockoutValue } from "hooks/useKnockoutValue";
import { NotificationDetailsProps } from "../notificationDetailsProvider";
import { AlertDetailsModal } from "../NotificationDetailsModal";
import { NotificationDetailsTable } from "../NotificationDetailsTable";
import { CellBytes, columnSizes } from "../NotificationDetailsCells";

type BlockingTombstoneDetails = Raven.Server.NotificationCenter.BlockingTombstoneDetails;
type TombstoneDeletionBlockerType = Raven.Server.Documents.ITombstoneAware.TombstoneDeletionBlockerType;

export default function BlockingTombstonesDetails({
    notification,
    actions,
}: NotificationDetailsProps<alert<"BlockingTombstones">>) {
    const details = useKnockoutValue(notification.details);
    const blockers = useMemo(() => details.BlockingTombstones ?? [], [details]);

    const columns = useMemo(
        (): ColumnDef<BlockingTombstoneDetails>[] => [
            {
                id: "Source",
                header: "Blockage source",
                accessorFn: (x) => `${getBlockerTypeDescription(x.BlockerType)} "${x.Source}"`,
                cell: ({ getValue, row }) => (
                    <a href={getSourceLink(row.original, notification.databaseName)} onClick={actions.close}>
                        {getValue() as string}
                    </a>
                ),
                meta: { relativeWidth: 40 },
            },
            {
                header: "Collection",
                accessorKey: "Collection",
                meta: { relativeWidth: 20 },
            },
            {
                header: "Tombstones count",
                accessorKey: "NumberOfTombstones",
                meta: { relativeWidth: 20 },
            },
            {
                header: "Size",
                accessorKey: "SizeOfTombstonesInBytes",
                cell: CellBytes,
                size: columnSizes.bytes,
                meta: { exportHeader: "Size (bytes)", filterPlaceholder: "e.g. > 5 MB" },
            },
        ],
        [notification, actions]
    );

    return (
        <AlertDetailsModal notification={notification} actions={actions} size="xxl">
            <NotificationDetailsTable
                notification={notification}
                data={blockers}
                columns={columns}
                itemName="source"
                itemNamePlural="sources"
            />
        </AlertDetailsModal>
    );
}

function getBlockerTypeDescription(type: TombstoneDeletionBlockerType): string {
    switch (type) {
        case "RavenEtl":
            return "RavenDB ETL";
        case "ExternalReplication":
            return "External Replication";
        case "SqlEtl":
            return "SQL ETL";
        case "SnowflakeEtl":
            return "Snowflake ETL";
        case "OlapEtl":
            return "OLAP ETL";
        case "ElasticSearchEtl":
            return "Elasticsearch ETL";
        case "PullReplicationAsHub":
            return "Replication Hub";
        case "PullReplicationAsSink":
            return "Replication Sink";
        case "QueueEtl":
            return "Queue ETL";
        default:
            return type;
    }
}

function getSourceLink(blocker: BlockingTombstoneDetails, databaseName: string): string {
    switch (blocker.BlockerType) {
        case "Index":
            return appUrl.forEditIndex(blocker.Source, databaseName);
        case "Backup":
            return appUrl.forEditPeriodicBackupTask(databaseName, "Backups", false, blocker.BlockerTaskId);
        case "ElasticSearchEtl":
            return appUrl.forEditElasticSearchEtl(databaseName, blocker.BlockerTaskId);
        case "ExternalReplication":
            return appUrl.forEditExternalReplication(databaseName, blocker.BlockerTaskId);
        case "OlapEtl":
            return appUrl.forEditOlapEtl(databaseName, blocker.BlockerTaskId);
        case "PullReplicationAsHub":
            return appUrl.forEditReplicationHub(databaseName, blocker.BlockerTaskId);
        case "PullReplicationAsSink":
            return appUrl.forEditReplicationSink(databaseName, blocker.BlockerTaskId);
        case "SqlEtl":
            return appUrl.forEditSqlEtl(databaseName, blocker.BlockerTaskId);
        case "SnowflakeEtl":
            return appUrl.forEditSnowflakeEtl(databaseName, blocker.BlockerTaskId);
        case "RavenEtl":
            return appUrl.forEditRavenEtl(databaseName, blocker.BlockerTaskId);
        default:
            return appUrl.forOngoingTasks(databaseName);
    }
}
