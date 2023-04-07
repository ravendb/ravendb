import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import React from "react";
import { DatabaseSharedInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { selectDatabaseState } from "components/common/shell/databasesSlice";
import genUtils from "common/generalUtils";
import { SizeOnDisk } from "components/pages/resources/databases/partials/SizeOnDisk";
import { DatabaseLoadError } from "components/pages/resources/databases/partials/DatabaseLoadError";

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
                        <i className="icon-node" /> Node
                    </div>
                )}
                <div>
                    <i className="icon-list" /> Documents
                </div>
                <div>
                    <i className="icon-warning" /> Indexing Errors
                </div>
                <div>
                    <i className="icon-indexing" /> Indexing Status
                </div>
                <div>
                    <i className="icon-alerts" /> Alerts
                </div>
                <div>
                    <i className="icon-info" /> Performance Hints
                </div>
                <div>
                    <i className="icon-storage" /> Size on disk
                </div>
                <div>
                    <i className="icon-recent" /> Uptime
                </div>
            </DistributionLegend>

            {dbState.map((localState) => {
                const shard = (
                    <div className="top shard">
                        {localState.location.shardNumber != null && (
                            <>
                                <i className="icon-shard" />
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
                            {!sharded && <i className="icon-node"></i>}

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

