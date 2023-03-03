import { DistributionItem, DistributionLegend, LocationDistribution } from "components/common/LocationDistribution";
import React, { useRef } from "react";
import { DatabaseSharedInfo } from "components/models/databases";
import classNames from "classnames";
import { useAppSelector } from "components/store";
import { selectDatabaseState } from "components/common/shell/databasesSlice";
import genUtils from "common/generalUtils";
import { UncontrolledTooltip } from "reactstrap";
import { PopoverWithHover } from "components/common/PopoverWithHover";

interface DatabaseDistributionProps {
    db: DatabaseSharedInfo;
}

export function DatabaseDistribution(props: DatabaseDistributionProps) {
    const { db } = props;
    const sharded = db.sharded;

    //TODO: expose other props and load error?

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
                                localState.data?.documentsCount
                            )}
                        </div>
                        <div className="entries">{localState.data?.indexingErrors}</div>
                        <div className="entries">{localState.data?.indexingStatus}</div>
                        <div className="entries">{localState.data?.alerts}</div>
                        <div className="entries">{localState.data?.performanceHints}</div>
                    </DistributionItem>
                );
            })}
        </LocationDistribution>
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
