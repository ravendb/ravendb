import React from "react";
import { ValidDatabasePropertiesPanelPopoverProps } from "./ValidDatabasePropertiesPanel";
import { Button } from "reactstrap";
import { Icon } from "components/common/Icon";

export default function ValidDatabasePropertiesPanelPerfHintsPopover({
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
                            {localTotal === 1 ? "hint" : "hints"}
                        </span>
                        {localTotal > 0 && (
                            <Button
                                type="button"
                                size="xs"
                                color="info"
                                className="rounded-pill"
                                onClick={openNotificationCenter}
                            >
                                <Icon icon="rocket" />
                                See {localTotal === 1 ? "hint" : "hints"}
                            </Button>
                        )}
                    </div>
                </>
            )}

            {remoteTopLevelStates.some((x) => !!x.performanceHints) && (
                <>
                    {isCurrentNodeRelevant && <hr className="my-2" />}
                    <strong className="d-block mb-1">Remote</strong>

                    {remoteTopLevelStates.map((x) => {
                        if (!x.performanceHints) {
                            return null;
                        }

                        return (
                            <div key={x.nodeTag} className="notifications-popover-grid mb-1">
                                <span>
                                    <Icon icon="node" color="node" /> {x.nodeTag}
                                </span>
                                <span>
                                    <strong>{x.performanceHints} </strong>
                                    {x.performanceHints === 1 ? "hint" : "hints"}
                                </span>
                                <a href={getServerNodeUrl(x.nodeTag)} className="no-decor" target="_blank">
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
