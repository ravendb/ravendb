import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import React, { useState } from "react";
import { DatabaseSharedInfo, ShardedDatabaseSharedInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import genUtils from "common/generalUtils";
import { SizeOnDisk } from "components/pages/resources/databases/partials/SizeOnDisk";
import { DatabaseLoadError } from "components/pages/resources/databases/partials/DatabaseLoadError";
import { Icon } from "components/common/Icon";
import { selectDatabaseState } from "components/pages/resources/databases/store/databasesViewSelectors";
import { DatabaseNodeSetItem } from "components/pages/resources/databases/partials/DatabaseNodeSetItem";

interface DatabaseDistributionProps {
    db: DatabaseSharedInfo;
}

function formatUptime(uptime: string) {
    return uptime ?? "Offline";
}

export function DatabaseDistribution(props: DatabaseDistributionProps) {
    const { db } = props;
    const sharded = db.sharded;
    const [hoveredShardNumber, setHoveredShardNumber] = useState<number | null>(null);
    const dbState = useAppSelector(selectDatabaseState(db.name));

    return (
        <LocationDistribution>
            <DistributionLegend>
                <div className="top"></div>
                {sharded && (
                    <div className="node">
                        <Icon icon="node" /> Node
                    </div>
                )}
                <div>
                    <Icon icon="list" /> Documents
                </div>
                <div>
                    <Icon icon="warning" /> Indexing Errors
                </div>
                <div>
                    <Icon icon="indexing" /> Indexing Status
                </div>
                <div>
                    <Icon icon="alerts" /> Alerts
                </div>
                <div>
                    <Icon icon="info" /> Performance Hints
                </div>
                <div>
                    <Icon icon="storage" /> Size on disk
                </div>
                <div>
                    <Icon icon="recent" /> Uptime
                </div>
            </DistributionLegend>

            {dbState.map((localState) => {
                const shard = (
                    <div className="top shard">
                        {localState.location.shardNumber != null && (
                            <>
                                <Icon icon="shard" />
                                {localState.location.shardNumber}
                            </>
                        )}
                    </div>
                );

                const shardNumber = localState.location.shardNumber;

                const nodesToUse = db.sharded
                    ? (db as ShardedDatabaseSharedInfo).shards[localState.location.shardNumber].nodes
                    : db.nodes;

                const node = nodesToUse.find((x) => x.tag === localState.location.nodeTag);

                const uptime = localState.data ? formatUptime(localState.data.upTime) : "";

                return (
                    <DistributionItem
                        key={genUtils.formatLocation(localState.location)}
                        loading={localState.status === "idle" || localState.status === "loading"}
                        className={classNames("distribution-item pb-2", {
                            [`shard-${localState.location.shardNumber}`]: sharded && shardNumber != null,
                            hovered: sharded ? shardNumber === hoveredShardNumber : false,
                        })}
                        onMouseEnter={() => setHoveredShardNumber(localState.location.shardNumber)}
                        onMouseLeave={() => setHoveredShardNumber(null)}
                    >
                        {sharded && shard}
                        <div className={classNames("node", { top: !sharded })}>
                            <DatabaseNodeSetItem node={node} />
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
