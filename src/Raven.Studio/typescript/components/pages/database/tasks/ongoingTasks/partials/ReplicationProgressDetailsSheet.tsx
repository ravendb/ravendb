import { OngoingTaskNodeReplicationProgressDetails } from "components/models/tasks";
import { loadStatus } from "components/models/common";
import { ChangeVectorDetails } from "components/pages/database/tasks/ongoingTasks/partials/ChangeVectorDetails";
import { Icon } from "components/common/Icon";
import { EmptySet } from "components/common/EmptySet";
import { NamedProgressItem } from "components/common/NamedProgress";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import Code from "components/common/Code";
import { getErrorHeadline } from "components/utils/common";
import useBoolean from "components/hooks/useBoolean";
import {
    ProgressDetailsSheetShell,
    useNodeSlider,
} from "components/pages/database/tasks/ongoingTasks/partials/ProgressDetailsSheetShell";
import "./ReplicationProgressDetailsSheet.scss";

export interface ReplicationProgressSheetNodeDetails {
    error: string;
    sourceDatabaseChangeVector: string;
    lastAcceptedChangeVectorFromDestination: string;
}

export interface ReplicationProgressSheetNodeInfo {
    location: databaseLocationSpecifier;
    status: loadStatus;
    details: ReplicationProgressSheetNodeDetails;
    progress: OngoingTaskNodeReplicationProgressDetails[];
}

interface ReplicationProgressDetailsSheetProps {
    taskType: string;
    taskName?: string;
    allNodes: ReplicationProgressSheetNodeInfo[];
    initialNodeIndex: number;
    onNodeChange?: (index: number) => void;
}

function ReplicationProgressCard({ label, progress }: { label: string; progress: Progress }) {
    return (
        <div className="replication-progress-card">
            <NamedProgressItem progress={progress}>{label}</NamedProgressItem>
        </div>
    );
}

function ReplicationError({ error }: { error: string }) {
    const headline = getErrorHeadline(error);
    return (
        <div className="vstack gap-1">
            <div className="text-danger fw-bold">
                <Icon icon="warning" color="danger" /> {headline}
            </div>
            {headline !== error && <Code code={error} language="plaintext" />}
        </div>
    );
}

interface ReplicationProgressBodyProps {
    nodeInfo: ReplicationProgressSheetNodeInfo;
    isDebugInfoExpanded: boolean;
    onToggleDebugInfo: () => void;
}

function ReplicationProgressBody({ nodeInfo, isDebugInfoExpanded, onToggleDebugInfo }: ReplicationProgressBodyProps) {
    if (nodeInfo.status === "failure") {
        const error = nodeInfo.details?.error;
        const headline = error ? getErrorHeadline(error) : "Unable to load task status";
        return (
            <div className="vstack gap-2 py-2">
                <div className="text-danger fw-bold">
                    <Icon icon="warning" color="danger" /> {headline}
                </div>
                {error && headline !== error && <Code code={error} language="plaintext" />}
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

    const sourceDatabaseCV = nodeInfo.details?.sourceDatabaseChangeVector;
    const lastAcceptedCV = nodeInfo.details?.lastAcceptedChangeVectorFromDestination;
    const hasDebugInfo = !!sourceDatabaseCV || !!lastAcceptedCV;

    if (progress.length === 0 && !hasError && !hasDebugInfo) {
        return <EmptySet compact>No progress data available.</EmptySet>;
    }

    return (
        <div className="vstack gap-3">
            {hasError && <ReplicationError error={nodeInfo.details.error} />}
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
                        <Button variant="link" size="xs" onClick={onToggleDebugInfo} className="p-0">
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
}

export function ReplicationProgressDetailsSheet(props: ReplicationProgressDetailsSheetProps) {
    const { taskType, taskName, allNodes, initialNodeIndex, onNodeChange } = props;

    const { selectedIndex, direction, handleNodeChange } = useNodeSlider(initialNodeIndex, onNodeChange);
    const { value: isDebugInfoExpanded, toggle: toggleDebugInfoExpanded } = useBoolean(false);
    const nodeInfo = allNodes[selectedIndex];

    return (
        <ProgressDetailsSheetShell
            title={taskName ? `${taskType} · ${taskName}` : taskType}
            locations={allNodes.map((n) => n.location)}
            selectedIndex={selectedIndex}
            direction={direction}
            onNodeChange={handleNodeChange}
        >
            <ReplicationProgressBody
                nodeInfo={nodeInfo}
                isDebugInfoExpanded={isDebugInfoExpanded}
                onToggleDebugInfo={toggleDebugInfoExpanded}
            />
        </ProgressDetailsSheetShell>
    );
}
