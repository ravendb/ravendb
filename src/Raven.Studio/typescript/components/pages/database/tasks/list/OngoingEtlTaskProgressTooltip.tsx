import React from "react";
import { PopoverWithHover } from "../../../../common/PopoverWithHover";
import { OngoingEtlTaskNodeInfo, OngoingTaskInfo, OngoingTaskNodeInfo } from "../../../../models/tasks";
import { NamedProgress, NamedProgressItem } from "../../../../common/NamedProgress";

interface OngoingTaskEtlProgressTooltipProps {
    target: string;
    nodeInfo: OngoingEtlTaskNodeInfo;
    task: OngoingTaskInfo;
    showPreview: (transformationName: string) => void;
}

export function OngoingEtlTaskProgressTooltip(props: OngoingTaskEtlProgressTooltipProps) {
    const { target, nodeInfo, showPreview } = props;
    return (
        <PopoverWithHover rounded target={target} placement="top" delay={100}>
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
                                    <i className="icon-preview" />
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
