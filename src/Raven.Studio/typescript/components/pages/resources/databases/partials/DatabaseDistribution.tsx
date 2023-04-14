import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import React from "react";
import { DatabaseSharedInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import genUtils from "common/generalUtils";
import { SizeOnDisk } from "components/pages/resources/databases/partials/SizeOnDisk";
import { DatabaseLoadError } from "components/pages/resources/databases/partials/DatabaseLoadError";
import { selectDatabaseState } from "components/common/shell/databaseSliceSelectors";
import { Icon } from "components/common/Icon";

interface DatabaseDistributionProps {
    db: DatabaseSharedInfo;
}

function formatUptime(uptime: string) {
    return uptime ?? "Offline";
}

export function DatabaseDistribution(props: DatabaseDistributionProps) {
    const { db } = props;
    const sharded = db.sharded;

    const dbState = useAppSelector(selectDatabaseState(db.name));

    return (
        <LocationDistribution>
            <DistributionLegend>
                <div className="top"></div>
                {sharded && (
                    <div className="node">
                        <Icon icon="node" className="me-1"></Icon> Node
                    </div>
                )}
                <div>
                    <Icon icon="list" className="me-1"></Icon> Documents
                </div>
                <div>
                    <Icon icon="warning" className="me-1"></Icon> Indexing Errors
                </div>
                <div>
                    <Icon icon="indexing" className="me-1"></Icon> Indexing Status
                </div>
                <div>
                    <Icon icon="alerts" className="me-1"></Icon> Alerts
                </div>
                <div>
                    <Icon icon="info" className="me-1"></Icon> Performance Hints
                </div>
                <div>
                    <Icon icon="storage" className="me-1"></Icon> Size on disk
                </div>
                <div>
                    <Icon icon="recent" className="me-1"></Icon> Uptime
                </div>
            </DistributionLegend>

            {dbState.map((localState) => {
                const shard = (
                    <div className="top shard">
                        {localState.location.shardNumber != null && (
                            <>
                                <Icon icon="shard" className="me-1"></Icon>
                                {localState.location.shardNumber}
                            </>
                        )}
                    </div>
                );

                const uptime = localState.data ? formatUptime(localState.data.upTime) : "";

                return (
                    <DistributionItem
                        key={genUtils.formatLocation(localState.location)}
                        loading={localState.status === "idle" || localState.status === "loading"}
                    >
                        {sharded && shard}
                        <div className={classNames("node", { top: !sharded })}>
                            {!sharded && <Icon icon="node" className="me-1"></Icon>}

                            {localState.location.nodeTag}
                        </div>
                        <div className="entries">
                            {localState.data?.loadError ? (
                                <DatabaseLoadError error={localState.data.loadError} />
                            ) : (
                                localState.data?.documentsCount.toLocaleString()
                            )}
                        </div>
                        <div className="entries">{localState.data?.indexingErrors?.toLocaleString()}</div>
                        <div className="entries">{localState.data?.indexingStatus}</div>
                        <div className="entries">{localState.data?.alerts?.toLocaleString()}</div>
                        <div className="entries">{localState.data?.performanceHints?.toLocaleString()}</div>
                        <div className="entries">
                            <SizeOnDisk info={localState.data} />
                        </div>
                        <div className="entries">{uptime}</div>
                    </DistributionItem>
                );
            })}
        </LocationDistribution>
    );
}
