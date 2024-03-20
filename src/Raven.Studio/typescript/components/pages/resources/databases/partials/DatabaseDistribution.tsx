import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import React, { useState } from "react";
import { DatabaseSharedInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import genUtils from "common/generalUtils";
import { SizeOnDisk } from "components/pages/resources/databases/partials/SizeOnDisk";
import { DatabaseLoadError } from "components/pages/resources/databases/partials/DatabaseLoadError";
import { Icon } from "components/common/Icon";
import { selectDatabaseState } from "components/pages/resources/databases/store/databasesViewSelectors";
import { DatabaseNodeSetItem } from "components/pages/resources/databases/partials/DatabaseNodeSetItem";
import DatabaseUtils from "components/utils/DatabaseUtils";

interface DatabaseDistributionProps {
    db: DatabaseSharedInfo;
}

export function DatabaseDistribution(props: DatabaseDistributionProps) {
    const { db } = props;
    const isSharded = db.isSharded;
    const [hoveredShardNumber, setHoveredShardNumber] = useState<number | null>(null);
    const dbState = useAppSelector(selectDatabaseState(db.name));

    return (
        <LocationDistribution>
            <DistributionLegend>
                <div className="top"></div>
                {isSharded && (
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

                const nodesToUse = db.isSharded ? db.shards[localState.location.shardNumber].nodes : db.nodes;
                const node = nodesToUse.find((x) => x.tag === localState.location.nodeTag);

                const uptime = localState.data ? DatabaseUtils.formatUptime(localState.data.upTime) : "";
                const isOfflineOrDisabled = uptime === "Offline" || db.isDisabled;

                return (
                    <DistributionItem
                        key={genUtils.formatLocation(localState.location)}
                        loading={localState.status === "idle" || localState.status === "loading"}
                        className={classNames("distribution-item pb-2", {
                            [`shard-${localState.location.shardNumber}`]: isSharded && shardNumber != null,
                            hovered: isSharded ? shardNumber === hoveredShardNumber : false,
                        })}
                        onMouseEnter={() => setHoveredShardNumber(localState.location.shardNumber)}
                        onMouseLeave={() => setHoveredShardNumber(null)}
                    >
                        {isSharded && shard}
                        <div className={classNames("node", { top: !isSharded })}>
                            <DatabaseNodeSetItem node={node} isOfflineOrDisabled={isOfflineOrDisabled} />
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
