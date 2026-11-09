import { useMemo, useState } from "react";
import { isEqual } from "lodash";
import moment from "moment";
import Badge from "react-bootstrap/Badge";
import { AboutViewHeading } from "components/common/AboutView";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useAppSelector } from "components/store";
import DatabaseUtils from "components/utils/DatabaseUtils";
import BackupDecisionLogFilters from "./partials/BackupDecisionLogFilters";
import BackupDecisionLogInfoHub from "./partials/BackupDecisionLogInfoHub";
import BackupDecisionLogStatusCards from "./partials/BackupDecisionLogStatusCards";
import BackupDecisionLogTable from "./partials/BackupDecisionLogTable";
import { BackupDecisionFilters, emptyBackupDecisionFilters } from "./backupDecisionLogTypes";
import { useBackupDecisionLog } from "./hooks/useBackupDecisionLog";
import { filterDecisions } from "./backupDecisionLogUtils";

export default function BackupDecisionLog() {
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);
    const [filters, setFilters] = useState<BackupDecisionFilters>(emptyBackupDecisionFilters);
    const { details, trackedDatabases, rows, oldestAt, isError, reload } = useBackupDecisionLog(filters.databases);

    const isFiltering = !isEqual(filters, emptyBackupDecisionFilters);
    const visibleRows = useMemo(() => filterDecisions(rows, filters), [rows, filters]);
    const taskNames = useMemo(
        () => [...new Set(rows.map((x) => x.TaskName).filter((x): x is string => x !== null))].sort(),
        [rows]
    );
    const databaseNames = useMemo(
        () => [...new Set((trackedDatabases ?? []).map((x) => DatabaseUtils.shardGroupKey(x)))],
        [trackedDatabases]
    );

    const renderContent = () => {
        if (!details && isError) {
            return <LoadError error="Unable to load the backup decision log" refresh={reload} />;
        }
        if (!details) {
            return <LoadingView />;
        }
        if (rows.length === 0 && !isFiltering) {
            return <EmptySet>No decisions yet</EmptySet>;
        }

        const entriesCount = details.Entries.length;
        const eventsText = pluralizeHelpers.pluralize(entriesCount, "event", "events", true);

        return (
            <>
                {entriesCount > 0 && (
                    <div className="text-muted mb-2">
                        {filters.databases.length > 0
                            ? `Showing ${entriesCount} ${eventsText} for the selected databases.`
                            : `Showing the newest ${entriesCount} ${eventsText}, back to ${moment(oldestAt).format("HH:mm")}. Choose a database to see further back.`}
                    </div>
                )}
                {visibleRows.length === 0 ? (
                    <EmptySet>No decisions match the current filters</EmptySet>
                ) : (
                    <BackupDecisionLogTable rows={visibleRows} />
                )}
            </>
        );
    };

    return (
        <div className="content-padding">
            <div className="hstack justify-content-between mb-4">
                <AboutViewHeading title="Backup Decision Log" icon="backups" marginBottom={0} />
                <BackupDecisionLogInfoHub />
            </div>
            <div className="hstack justify-content-end mb-4">
                <PopoverWithHoverWrapper
                    message={`Shows decisions made by node ${localNodeTag} only. Each node keeps its own log.`}
                >
                    <Badge bg="secondary" pill>
                        <Icon icon="node" />
                        Node {localNodeTag}
                    </Badge>
                </PopoverWithHoverWrapper>
            </div>
            {details && <BackupDecisionLogStatusCards queue={details.Queue} rows={rows} oldestAt={oldestAt} />}
            {trackedDatabases && (
                <BackupDecisionLogFilters
                    filters={filters}
                    onChange={setFilters}
                    taskNames={taskNames}
                    databaseNames={databaseNames}
                    matchCount={visibleRows.length}
                />
            )}
            {renderContent()}
        </div>
    );
}
