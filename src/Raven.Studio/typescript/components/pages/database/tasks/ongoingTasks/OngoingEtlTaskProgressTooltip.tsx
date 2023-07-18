import React from "react";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { OngoingEtlTaskNodeInfo, OngoingTaskInfo } from "components/models/tasks";
import { NamedProgress, NamedProgressItem } from "components/common/NamedProgress";
import { Icon } from "components/common/Icon";

interface OngoingTaskEtlProgressTooltipProps {
    target: HTMLElement;
    nodeInfo: OngoingEtlTaskNodeInfo;
    task: OngoingTaskInfo;
    showPreview: (transformationName: string) => void;
}

export function OngoingEtlTaskProgressTooltip(props: OngoingTaskEtlProgressTooltipProps) {
    const { target, nodeInfo, showPreview } = props;

    if (nodeInfo.status === "failure") {
        return (
            <PopoverWithHover target={target} placement="top">
                <div className="text-danger flex-horizontal">
                    <div className="flex-start text-warning">
                        <Icon icon="warning" margin="m-0" />
                    </div>
                    <div>
                        <div>Unable to load task status:</div>
                        <div>{nodeInfo.details.error}</div>
                    </div>
                </div>
            </PopoverWithHover>
        );
    }

    if (nodeInfo.status !== "success") {
        return null;
    }

    return (
        <PopoverWithHover rounded="true" target={target} placement="top">
            <div className="ongoing-tasks-details-tooltip">
                {nodeInfo.etlProgress &&
                    nodeInfo.etlProgress.map((transformationScriptProgress) => {
                        const nameNode = (
                            <div>
                                {transformationScriptProgress.transformationName}
                                <button
                                    type="button"
                                    className="btn btn-link btn-sm margin-left-xs"
                                    title="Show script preview"
                                    onClick={() => showPreview(transformationScriptProgress.transformationName)}
                                >
                                    <Icon icon="preview" margin="m-0" />
                                </button>
                            </div>
                        );

                        return (
                            <NamedProgress name={nameNode} key={transformationScriptProgress.transformationName}>
                                <NamedProgressItem progress={transformationScriptProgress.documents}>
                                    documents
                                </NamedProgressItem>
                                <NamedProgressItem progress={transformationScriptProgress.documentTombstones}>
                                    tombstones
                                </NamedProgressItem>
                                {transformationScriptProgress.counterGroups.total > 0 && (
                                    <NamedProgressItem progress={transformationScriptProgress.counterGroups}>
                                        counters
                                    </NamedProgressItem>
                                )}
                            </NamedProgress>
                        );
                    })}
            </div>
        </PopoverWithHover>
    );
}
