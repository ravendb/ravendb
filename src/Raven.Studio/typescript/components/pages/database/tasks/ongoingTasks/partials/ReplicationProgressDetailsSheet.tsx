import React, { useState } from "react";
import { ViewSheet } from "components/common/splitView/ViewSheet";
import {
    OngoingTaskAbstractReplicationNodeInfoDetails,
    OngoingReplicationProgressAwareTaskNodeInfo,
    OngoingTaskNodeReplicationProgressDetails,
} from "components/models/tasks";
import { ChangeVectorDetails } from "components/pages/database/tasks/ongoingTasks/partials/ChangeVectorDetails";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import Code from "components/common/Code";
import { NodeLocationTabs } from "components/pages/database/tasks/ongoingTasks/partials/NodeLocationSelect";
import classNames from "classnames";
import { AnimatePresence, motion } from "motion/react";
import { getErrorHeadline } from "components/utils/common";
import "./ReplicationProgressDetailsSheet.scss";

type Direction = 1 | -1;

const slideVariants = {
    enter: (d: Direction) => ({ x: `${d * 100}%` }),
    center: { x: 0 },
    exit: (d: Direction) => ({ x: `${d * -100}%` }),
};

interface ReplicationProgressDetailsSheetProps {
    taskType: string;
    taskName?: string;
    allNodes: OngoingReplicationProgressAwareTaskNodeInfo<OngoingTaskAbstractReplicationNodeInfoDetails>[];
    initialNodeIndex: number;
    onNodeChange?: (index: number) => void;
}

interface ProgressValue {
    total: number;
    processed: number;
}

function ReplicationProgressCard({ label, progress }: { label: string; progress: ProgressValue }) {
    const { total, processed } = progress;
    const percentage = total === 0 ? 100 : Math.floor((processed * 100.0) / total);
    const completed = total === processed;
    const remaining = total - processed;
    const title = completed
        ? `Processed all items (${processed.toLocaleString()})`
        : `Processed ${processed.toLocaleString()} out of ${total.toLocaleString()} (${remaining.toLocaleString()} left)`;

    return (
        <div className="replication-progress-card" title={title}>
            <div className="d-flex justify-content-between align-items-center">
                <div className="small-label">{label}</div>
                <strong className="progress-percentage">{percentage}%</strong>
            </div>
            <div className="progress">
                <div className={classNames("progress-bar", { completed })} style={{ width: percentage + "%" }} />
            </div>
        </div>
    );
}

export function ReplicationProgressDetailsSheet(props: ReplicationProgressDetailsSheetProps) {
    const { taskType, taskName, allNodes, initialNodeIndex, onNodeChange } = props;

    const [selectedIndex, setSelectedIndex] = useState(initialNodeIndex);
    const [direction, setDirection] = useState<Direction>(1);
    const [prevInitialNodeIndex, setPrevInitialNodeIndex] = useState(initialNodeIndex);

    if (initialNodeIndex !== prevInitialNodeIndex) {
        setPrevInitialNodeIndex(initialNodeIndex);
        setDirection(initialNodeIndex > selectedIndex ? 1 : -1);
        setSelectedIndex(initialNodeIndex);
    }

    const handleNodeChange = (index: number) => {
        setDirection(index > selectedIndex ? 1 : -1);
        setSelectedIndex(index);
        onNodeChange?.(index);
    };
    const [isDebugInfoExpanded, setIsDebugInfoExpanded] = useState(false);
    const nodeInfo = allNodes[selectedIndex];

    const renderError = (error: string) => (
        <div className="vstack gap-1">
            <div className="text-danger fw-bold">
                <Icon icon="warning" color="danger" /> {getErrorHeadline(error)}
            </div>
            <Code code={error} language="plaintext" />
        </div>
    );

    const renderBody = () => {
        if (nodeInfo.status === "failure") {
            const error = nodeInfo.details?.error;
            return (
                <div className="vstack gap-2 py-2">
                    <div className="text-danger fw-bold">
                        <Icon icon="warning" color="danger" />{" "}
                        {error ? getErrorHeadline(error) : "Unable to load task status"}
                    </div>
                    {error && <Code code={error} language="plaintext" />}
                </div>
            );
        }

        if (nodeInfo.status === "loading" || nodeInfo.status === "idle") {
            return (
                <div className="d-flex justify-content-center py-4">
                    <Spinner animation="border" />
                </div>
            );
        }

        const hasError = !!nodeInfo.details?.error;
        const progress: OngoingTaskNodeReplicationProgressDetails[] = nodeInfo.progress ?? [];

        if (progress.length === 0 && !hasError) {
            return <div className="text-muted text-center py-3">No progress data available.</div>;
        }

        const sourceDatabaseCV = nodeInfo.details?.sourceDatabaseChangeVector;
        const lastAcceptedCV = nodeInfo.details?.lastAcceptedChangeVectorFromDestination;
        const hasDebugInfo = !!sourceDatabaseCV || !!lastAcceptedCV;

        return (
            <div className="vstack gap-3">
                {hasError && renderError(nodeInfo.details.error)}
                {progress.map((singleProgress, index) => (
                    <div key={"progress-" + index} className="vstack gap-2">
                        <h4 className="mb-0">Replication process overview</h4>
                        <div className="replication-progress-grid">
                            <ReplicationProgressCard label="documents" progress={singleProgress.documents} />
                            <ReplicationProgressCard label="tombstones" progress={singleProgress.documentTombstones} />
                            <ReplicationProgressCard label="revisions" progress={singleProgress.revisions} />
                            <ReplicationProgressCard label="attachments" progress={singleProgress.attachments} />
                            <ReplicationProgressCard label="counters" progress={singleProgress.counterGroups} />
                            <ReplicationProgressCard label="time-series" progress={singleProgress.timeSeries} />
                            <ReplicationProgressCard
                                label="time-series deleted ranges"
                                progress={singleProgress.timeSeriesDeletedRanges}
                            />
                        </div>
                        {index !== progress.length - 1 && <hr className="mt-2 mb-0" />}
                    </div>
                ))}
                {hasDebugInfo && (
                    <div className="vstack gap-2">
                        <div className="d-flex align-items-center justify-content-between">
                            <h4 className="mb-0">Debug info</h4>
                            <Button
                                variant="link"
                                size="xs"
                                onClick={() => setIsDebugInfoExpanded((prev) => !prev)}
                                className="p-0"
                            >
                                {isDebugInfoExpanded ? (
                                    <>
                                        <Icon icon="collapse-vertical" margin="me-1" />
                                        Collapse
                                    </>
                                ) : (
                                    <>
                                        <Icon icon="expand-vertical" margin="me-1" />
                                        Expand
                                    </>
                                )}
                            </Button>
                        </div>
                        {isDebugInfoExpanded && (
                            <ChangeVectorDetails
                                sourceDatabaseChangeVector={sourceDatabaseCV}
                                lastAcceptedChangeVectorFromDestination={lastAcceptedCV}
                            />
                        )}
                    </div>
                )}
            </div>
        );
    };

    return (
        <ViewSheet>
            <ViewSheet.Header isPinHidden isCloseHidden className="pb-0">
                <div className="vstack gap-2 w-100">
                    <div className="d-flex justify-content-between align-items-center">
                        <div className="d-flex align-items-center gap-1">
                            <Icon icon="ongoing-tasks" margin="me-0" color="primary" />
                            <h4 className="mb-0">{taskName ?? taskType} details</h4>
                        </div>
                        <div className="d-flex align-items-center">
                            <ViewSheet.PinButton />
                            <ViewSheet.CloseButton />
                        </div>
                    </div>
                    <div className="d-flex align-items-center">
                        <NodeLocationTabs
                            locations={allNodes.map((n) => n.location)}
                            selectedIndex={selectedIndex}
                            onChange={handleNodeChange}
                        />
                    </div>
                </div>
            </ViewSheet.Header>
            <ViewSheet.Body className="p-3">
                <div style={{ overflow: "hidden", position: "relative" }}>
                    <AnimatePresence mode="popLayout" custom={direction} initial={false}>
                        <motion.div
                            key={selectedIndex}
                            custom={direction}
                            variants={slideVariants}
                            initial="enter"
                            animate="center"
                            exit="exit"
                            transition={{ duration: 0.3, ease: "easeInOut" }}
                        >
                            {renderBody()}
                        </motion.div>
                    </AnimatePresence>
                </div>
            </ViewSheet.Body>
        </ViewSheet>
    );
}
