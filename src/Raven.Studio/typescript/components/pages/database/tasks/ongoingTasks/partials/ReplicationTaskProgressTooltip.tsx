import React from "react";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { OngoingTaskNodeReplicationProgressDetails } from "components/models/tasks";
import { NamedProgress, NamedProgressItem } from "components/common/NamedProgress";
import { ChangeVectorDetails } from "components/pages/database/tasks/ongoingTasks/partials/ChangeVectorDetails";
import { NodeInfoFailure } from "components/pages/database/tasks/ongoingTasks/partials/NodeInfoFailure";
import { loadStatus } from "components/models/common";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";

interface ReplicationTaskProgressTooltipProps {
    target: HTMLElement;
    status: loadStatus;
    hasError: boolean;
    progress: OngoingTaskNodeReplicationProgressDetails[];
    sourceDatabaseChangeVector: string;
    lastAcceptedChangeVectorFromDestination: string;
    toggleErrorModal: () => void;
}

export function ReplicationTaskProgressTooltip(props: ReplicationTaskProgressTooltipProps) {
    const {
        target,
        progress,
        status,
        hasError,
        sourceDatabaseChangeVector,
        lastAcceptedChangeVectorFromDestination,
        toggleErrorModal,
    } = props;

    if (status === "failure") {
        return <NodeInfoFailure target={target} openErrorModal={toggleErrorModal} />;
    }

    if (status !== "success") {
        return null;
    }

    const hasAnyDetailsToShow =
        progress?.length > 0 || sourceDatabaseChangeVector || lastAcceptedChangeVectorFromDestination || hasError;

    if (!hasAnyDetailsToShow) {
        return null;
    }

    return (
        <PopoverWithHover rounded="true" target={target} placement="top">
            <div className="vstack gap-3 py-2">
                <ChangeVectorDetails
                    sourceDatabaseChangeVector={sourceDatabaseChangeVector}
                    lastAcceptedChangeVectorFromDestination={lastAcceptedChangeVectorFromDestination}
                />
                {hasError && (
                    <div className="text-center">
                        <Button variant="danger" key="button" onClick={toggleErrorModal} className="rounded-pill">
                            Open error in modal <Icon icon="newtab" margin="ms-1" />
                        </Button>
                    </div>
                )}
                {progress &&
                    progress.map((singleProgress, index) => {
                        return (
                            <div key={"progress-" + index} className="vstack">
                                <NamedProgress name="Replication">
                                    <NamedProgressItem progress={singleProgress.documents}>documents</NamedProgressItem>
                                    <NamedProgressItem progress={singleProgress.documentTombstones}>
                                        tombstones
                                    </NamedProgressItem>
                                    <NamedProgressItem progress={singleProgress.revisions}>revisions</NamedProgressItem>
                                    <NamedProgressItem progress={singleProgress.attachments}>
                                        attachments
                                    </NamedProgressItem>
                                    <NamedProgressItem progress={singleProgress.counterGroups}>
                                        counters
                                    </NamedProgressItem>
                                    <NamedProgressItem progress={singleProgress.timeSeries}>
                                        time-series
                                    </NamedProgressItem>
                                    <NamedProgressItem progress={singleProgress.timeSeriesDeletedRanges}>
                                        time-series deleted ranges
                                    </NamedProgressItem>
                                </NamedProgress>
                                {index !== progress.length - 1 && <hr className="mt-2 mb-0" />}
                            </div>
                        );
                    })}
            </div>
        </PopoverWithHover>
    );
}
