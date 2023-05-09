import React from "react";
import { ValidDatabasePropertiesPanelPopoverProps } from "./ValidDatabasePropertiesPanel";
import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";

export default function ValidDatabasePropertiesPanelAlertsPopover({
    isCurrentNodeRelevant,
    localNodeTag,
    localTotal,
    remoteTopLevelStates,
    openNotificationCenter,
    getServerNodeUrl,
}: ValidDatabasePropertiesPanelPopoverProps) {
    return (
        <div className="p-3 notifications-popover">
            {isCurrentNodeRelevant && (
                <>
                    <strong className="d-block mb-1">Local</strong>
                    <div className="notifications-popover-grid">
                        <span>
                            <Icon icon="node" color="node" /> {localNodeTag}
                        </span>
                        <span>
                            <strong>{localTotal} </strong>
                            {localTotal === 1 ? "alert" : "alerts"}
                        </span>
                        <Button
                            type="button"
                            size="xs"
                            color="warning"
                            className="rounded-pill"
                            onClick={openNotificationCenter}
                        >
                            <Icon icon="alert" />
                            See {localTotal === 1 ? "alert" : "alerts"}
                        </Button>
                    </div>
                </>
            )}

            {remoteTopLevelStates.some((x) => !!x.performanceHints) && (
                <>
                    {isCurrentNodeRelevant && <hr className="my-2" />}
                    <strong className="d-block mb-1">Remote</strong>

                    {remoteTopLevelStates.map((x) => {
                        if (!x.alerts) {
                            return null;
                        }

                        return (
                            <div key={x.nodeTag} className="notifications-popover-grid mb-1">
                                <span>
                                    <Icon icon="node" color="node" /> {x.nodeTag}
                                </span>
                                <span>
                                    <strong>{x.alerts}</strong> {x.alerts === 1 ? "alert" : "alerts"}
                                </span>
                                <a href={getServerNodeUrl(x.nodeTag)} className="no-decor">
                                    <Button type="button" size="xs" color="node" className="rounded-pill">
                                        <Icon icon="newtab" />
                                        Open node
                                    </Button>
                                </a>
                            </div>
                        );
                    })}
                </>
            )}
        </div>
    );
}
