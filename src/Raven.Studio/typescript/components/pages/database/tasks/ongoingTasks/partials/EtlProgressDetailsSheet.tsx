import React, { useState } from "react";
import { ViewSheet } from "components/common/splitView/ViewSheet";
import {
    AnyEtlOngoingTaskInfo,
    OngoingEtlTaskNodeInfo,
    OngoingTaskNodeEtlProgressDetails,
} from "components/models/tasks";
import { NamedProgress, NamedProgressItem } from "components/common/NamedProgress";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Spinner from "react-bootstrap/Spinner";
import copyToClipboard from "common/copyToClipboard";
import Code from "components/common/Code";
import { NodeLocationTabs } from "components/pages/database/tasks/ongoingTasks/partials/NodeLocationSelect";
import { AnimatePresence, motion } from "motion/react";
import {
    getEtlTaskTypeIcon,
    getEtlTaskTypeLabel,
} from "components/pages/database/tasks/ongoingTasks/panels/etlPanelUtils";
import AceEditor from "components/common/ace/AceEditor";
import useBoolean from "hooks/useBoolean";
import { useServices } from "hooks/useServices";
import { useAsync } from "react-async-hook";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import TaskUtils from "components/utils/TaskUtils";
import { getErrorHeadline } from "components/utils/common";
import "./EtlProgressDetailsSheet.scss";

type Direction = 1 | -1;

const slideVariants = {
    enter: (d: Direction) => ({ x: `${d * 100}%` }),
    center: { x: 0 },
    exit: (d: Direction) => ({ x: `${d * -100}%` }),
};

interface EtlProgressDetailsSheetProps {
    task: AnyEtlOngoingTaskInfo;
    allNodes: OngoingEtlTaskNodeInfo[];
    initialNodeIndex: number;
    onNodeChange?: (index: number) => void;
}

export function EtlProgressDetailsSheet(props: EtlProgressDetailsSheetProps) {
    const { task, allNodes, initialNodeIndex, onNodeChange } = props;

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
    const nodeInfo = allNodes[selectedIndex];

    return (
        <ViewSheet className="etl-progress-details-sheet">
            <ViewSheet.Header isPinHidden isCloseHidden className="pb-0">
                <div className="vstack gap-2 w-100">
                    <div className="d-flex justify-content-between align-items-center">
                        <div className="d-flex align-items-center gap-1">
                            <Icon icon="ongoing-tasks" margin="me-0" color="primary" />
                            <h4 className="mb-0">
                                {task.shared.taskName ?? getEtlTaskTypeLabel(task.shared.taskType)} details
                            </h4>
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
                            <EtlProgressBody task={task} nodeInfo={nodeInfo} />
                        </motion.div>
                    </AnimatePresence>
                </div>
            </ViewSheet.Body>
        </ViewSheet>
    );
}

function useEtlScriptDefinition(task: AnyEtlOngoingTaskInfo, enabled: boolean) {
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return useAsync(async () => {
        if (!enabled) {
            return null;
        }

        const ongoingTaskType = TaskUtils.studioTaskTypeToTaskType(task.shared.taskType);
        const etlType = TaskUtils.taskTypeToEtlType(ongoingTaskType);
        return tasksService.getEtlTaskInfo(databaseName, etlType, task.shared.taskId);
    }, [enabled, databaseName, task.shared.taskId, task.shared.taskType]);
}

function DetailRow({ label, children }: { label: string; children: React.ReactNode }) {
    return (
        <div className="detail-row">
            <span className="small">{label}</span>
            <span className="text-truncate">{children}</span>
        </div>
    );
}

interface ScriptPreviewProps {
    task: AnyEtlOngoingTaskInfo;
    transformationName: string;
}

function ScriptPreview({ task, transformationName }: ScriptPreviewProps) {
    const { value: isOpen, toggle } = useBoolean(false);
    const asyncTaskDefinition = useEtlScriptDefinition(task, isOpen);

    const transform = asyncTaskDefinition.result?.Configuration?.Transforms?.find((x) => x.Name === transformationName);

    return (
        <div>
            <div className="d-flex justify-content-between align-items-center">
                <h5 className="mb-0">Script preview</h5>
                <Button variant="link" size="xs" className="p-0" onClick={toggle}>
                    <Icon icon={isOpen ? "collapse-vertical" : "expand-vertical"} margin="me-1" />
                    {isOpen ? "Collapse" : "Expand"}
                </Button>
            </div>
            <AnimatePresence initial={false}>
                {isOpen && (
                    <motion.div
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: "auto", opacity: 1 }}
                        exit={{ height: 0, opacity: 0 }}
                        transition={{ duration: 0.2, ease: "easeInOut" }}
                        style={{ overflow: "hidden" }}
                        className="script-preview mt-2"
                    >
                        {asyncTaskDefinition.loading ? (
                            <div className="d-flex justify-content-center py-3">
                                <Spinner animation="border" size="sm" />
                            </div>
                        ) : (
                            <>
                                <DetailRow label="Task type">
                                    <span className="d-flex align-items-center gap-1">
                                        <Icon icon={getEtlTaskTypeIcon(task.shared.taskType)} margin="m-0" />
                                        {getEtlTaskTypeLabel(task.shared.taskType)}
                                    </span>
                                </DetailRow>
                                <DetailRow label="Task name">{task.shared.taskName}</DetailRow>
                                <DetailRow label="Transformation name">{transformationName}</DetailRow>
                                <DetailRow label="Collections">
                                    {transform?.ApplyToAllDocuments
                                        ? "All collections"
                                        : (transform?.Collections ?? []).join(", ") || "-"}
                                </DetailRow>
                                <div className="pt-2">
                                    <div className=" small">Transform script</div>
                                    {transform?.Script ? (
                                        <AceEditor
                                            mode="javascript"
                                            value={transform.Script}
                                            readOnly
                                            height="150px"
                                            isFullScreenLabelHidden
                                        />
                                    ) : (
                                        <div className="text-muted small">
                                            No transform script has been defined. Sending documents without any
                                            modifications.
                                        </div>
                                    )}
                                </div>
                            </>
                        )}
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
}

interface ScriptSectionProps {
    task: AnyEtlOngoingTaskInfo;
    scriptProgress: OngoingTaskNodeEtlProgressDetails;
    isInitiallyExpanded: boolean;
}

function ScriptSection({ task, scriptProgress, isInitiallyExpanded }: ScriptSectionProps) {
    const { value: isExpanded, toggle: toggleExpanded } = useBoolean(isInitiallyExpanded);

    return (
        <div className="well rounded mb-2 p-3">
            <div className="d-flex justify-content-between align-items-center cursor-pointer" onClick={toggleExpanded}>
                <h4 className="mb-0">{scriptProgress.transformationName}</h4>
                <Icon icon={isExpanded ? "collapse-vertical" : "expand-vertical"} margin="m-0" />
            </div>
            <AnimatePresence initial={false}>
                {isExpanded && (
                    <motion.div
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: "auto", opacity: 1 }}
                        exit={{ height: 0, opacity: 0 }}
                        transition={{ duration: 0.2, ease: "easeInOut" }}
                        style={{ overflow: "hidden" }}
                        className="pt-2"
                    >
                        {scriptProgress.transactionalId && (
                            <div className="d-flex align-items-center gap-1 mb-2">
                                <Icon icon="identities" margin="m-0" />
                                <small className="small">Transactional ID</small>
                                <div className="d-flex align-items-center gap-1 small">
                                    <span className="text-truncate" title={scriptProgress.transactionalId}>
                                        {scriptProgress.transactionalId}
                                    </span>
                                    <Button
                                        variant="link"
                                        size="xs"
                                        className="p-0 flex-shrink-0"
                                        title="Copy to clipboard"
                                        onClick={() =>
                                            copyToClipboard.copy(
                                                scriptProgress.transactionalId!,
                                                "Transactional Id was copied to clipboard."
                                            )
                                        }
                                    >
                                        <Icon icon="copy" margin="m-0" />
                                    </Button>
                                </div>
                            </div>
                        )}
                        <NamedProgress name={null} vertical>
                            <NamedProgressItem progress={scriptProgress.documents}>documents</NamedProgressItem>
                            <NamedProgressItem progress={scriptProgress.documentTombstones}>
                                tombstones
                            </NamedProgressItem>
                            {scriptProgress.counterGroups.total > 0 && (
                                <NamedProgressItem progress={scriptProgress.counterGroups}>counters</NamedProgressItem>
                            )}
                        </NamedProgress>
                        <hr className="script-separator" />
                        <ScriptPreview task={task} transformationName={scriptProgress.transformationName} />
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
}

function EtlError({ error }: { error: string }) {
    return (
        <div className="vstack gap-1">
            <div className="text-danger fw-bold">
                <Icon icon="warning" color="danger" /> {getErrorHeadline(error)}
            </div>
            <Code code={error} language="plaintext" />
        </div>
    );
}

interface EtlProgressBodyProps {
    task: AnyEtlOngoingTaskInfo;
    nodeInfo: OngoingEtlTaskNodeInfo;
}

function EtlProgressBody({ task, nodeInfo }: EtlProgressBodyProps) {
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
    const progress = nodeInfo.etlProgress ?? [];

    if (progress.length === 0 && !hasError) {
        return <div className="text-muted text-center py-3">No progress data available.</div>;
    }

    const isSingleScript = progress.length === 1;

    return (
        <div className="vstack gap-3">
            {hasError && <EtlError error={nodeInfo.details.error} />}
            {isSingleScript ? (
                <div>
                    <h4 className="mb-2">{progress[0].transformationName}</h4>
                    {progress[0].transactionalId && (
                        <div className="d-flex align-items-center gap-1 mb-2">
                            <Icon icon="identities" margin="m-0" />
                            <small className="small">Transactional ID</small>
                            <div className="d-flex align-items-center gap-1 small">
                                <span className="text-truncate" title={progress[0].transactionalId}>
                                    {progress[0].transactionalId}
                                </span>
                                <Button
                                    variant="link"
                                    size="xs"
                                    className="p-0 flex-shrink-0"
                                    title="Copy to clipboard"
                                    onClick={() =>
                                        copyToClipboard.copy(
                                            progress[0].transactionalId,
                                            "Transactional Id was copied to clipboard."
                                        )
                                    }
                                >
                                    <Icon icon="copy" margin="m-0" />
                                </Button>
                            </div>
                        </div>
                    )}
                    <NamedProgress name={null} vertical>
                        <NamedProgressItem progress={progress[0].documents}>documents</NamedProgressItem>
                        <NamedProgressItem progress={progress[0].documentTombstones}>tombstones</NamedProgressItem>
                        {progress[0].counterGroups.total > 0 && (
                            <NamedProgressItem progress={progress[0].counterGroups}>counters</NamedProgressItem>
                        )}
                    </NamedProgress>
                    <hr className="script-separator" />
                    <ScriptPreview task={task} transformationName={progress[0].transformationName} />
                </div>
            ) : (
                <div>
                    {progress.map((scriptProgress) => (
                        <ScriptSection
                            key={scriptProgress.transformationName}
                            task={task}
                            scriptProgress={scriptProgress}
                            isInitiallyExpanded
                        />
                    ))}
                </div>
            )}
        </div>
    );
}
