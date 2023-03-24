import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import React, { useRef } from "react";
import { DatabaseLocalInfo, DatabaseSharedInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { selectDatabaseState } from "components/common/shell/databasesSlice";
import genUtils from "common/generalUtils";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { UncontrolledTooltip } from "reactstrap";

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

function SizeOnDisk(props: { info: DatabaseLocalInfo }) {
    const { info } = props;

    const divRef = useRef<HTMLDivElement>();

    if (!info) {
        return null;
    }
    const tempBufferSize = info.tempBuffersSize?.SizeInBytes ?? 0;
    const totalSize = info.totalSize?.SizeInBytes ?? 0;
    const grandTotalSize = tempBufferSize + totalSize;

    return (
        <div>
            <div ref={divRef}>{genUtils.formatBytesToSize(grandTotalSize)}</div>
            {divRef.current && (
                <UncontrolledTooltip target={divRef.current}>
                    Data: <strong>{genUtils.formatBytesToSize(totalSize)}</strong>
                    <br />
                    Temp: <strong>{genUtils.formatBytesToSize(tempBufferSize)}</strong>
                    <br />
                    Total: <strong>{genUtils.formatBytesToSize(grandTotalSize)}</strong>
                </UncontrolledTooltip>
            )}
        </div>
    );
}

function DatabaseLoadError(props: { error: string }) {
    const tooltipRef = useRef<HTMLElement>();

    return (
        <strong className="text-danger" ref={tooltipRef}>
            <i className="icon-exclamation" /> Load error
            <PopoverWithHover target={tooltipRef.current} placement="top">
                <div className="p-2">Unable to load database: {props.error}</div>
            </PopoverWithHover>
        </strong>
    );
}
