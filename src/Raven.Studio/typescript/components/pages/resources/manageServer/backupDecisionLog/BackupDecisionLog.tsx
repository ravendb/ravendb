import { useState } from "react";
import { isEqual } from "lodash";
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
import BackupDecisionLogFilters from "./partials/BackupDecisionLogFilters";
import BackupDecisionLogInfoHub from "./partials/BackupDecisionLogInfoHub";
import BackupDecisionLogStatusCards from "./partials/BackupDecisionLogStatusCards";
import BackupDecisionLogTable from "./partials/BackupDecisionLogTable";
import { BackupDecisionFilters, emptyBackupDecisionFilters } from "./backupDecisionLogTypes";
import { useBackupDecisionLog } from "./hooks/useBackupDecisionLog";

export default function BackupDecisionLog() {
    const localNodeTag = useAppSelector(clusterSelectors.localNodeTag);
    const [filters, setFilters] = useState<BackupDecisionFilters>(emptyBackupDecisionFilters);
    const { details, lastDetails, isError, reconnect } = useBackupDecisionLog(filters);

    const isFiltering = !isEqual(filters, emptyBackupDecisionFilters);

    const renderContent = () => {
        if (!details && isError) {
            return <LoadError error="Unable to load the backup decision log" refresh={reconnect} />;
        }
        if (!details) {
            return <LoadingView />;
        }
        if (details.Entries.length === 0) {
            return <EmptySet>{isFiltering ? "No decisions match the current filters" : "No decisions yet"}</EmptySet>;
        }

        const shownCount = details.Entries.length;
        const eventsText = pluralizeHelpers.pluralize(details.TotalResults, "event", "events", true);

        return (
            <>
                <div className="text-muted mb-2">
                    {shownCount < details.TotalResults
                        ? `Showing the newest ${shownCount} of ${details.TotalResults} ${eventsText}. Use filters to see older ones.`
                        : `Showing ${shownCount} ${eventsText}.`}
                </div>
                <BackupDecisionLogTable entries={details.Entries} />
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
            {lastDetails && (
                <>
                    <BackupDecisionLogStatusCards
                        summary={lastDetails.Summary}
                        runnerStartTime={lastDetails.RunnerStartTime}
                    />
                    <BackupDecisionLogFilters
                        filters={filters}
                        onChange={setFilters}
                        databaseNames={lastDetails.Databases}
                        tasks={lastDetails.Tasks}
                        matchCount={details?.TotalResults ?? null}
                    />
                </>
            )}
            {renderContent()}
        </div>
    );
}
