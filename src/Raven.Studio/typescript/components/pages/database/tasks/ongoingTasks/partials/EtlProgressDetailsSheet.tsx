import React, { useState } from "react";
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
import { AnimatePresence, motion } from "motion/react";
import AceEditor from "components/common/ace/AceEditor";
import useBoolean from "hooks/useBoolean";
import { useServices } from "hooks/useServices";
import { useAsync } from "react-async-hook";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import TaskUtils from "components/utils/TaskUtils";
import { getErrorHeadline } from "components/utils/common";
import recentError from "common/notifications/models/recentError";
import {
    ProgressDetailsSheetShell,
    useNodeSlider,
} from "components/pages/database/tasks/ongoingTasks/partials/ProgressDetailsSheetShell";
import "./EtlProgressDetailsSheet.scss";

interface EtlProgressDetailsSheetProps {
    task: AnyEtlOngoingTaskInfo;
    allNodes: OngoingEtlTaskNodeInfo[];
    initialNodeIndex: number;
    onNodeChange?: (index: number) => void;
}

export function EtlProgressDetailsSheet(props: EtlProgressDetailsSheetProps) {
    const { task, allNodes, initialNodeIndex, onNodeChange } = props;

    const { selectedIndex, direction, handleNodeChange } = useNodeSlider(initialNodeIndex, onNodeChange);
    const nodeInfo = allNodes[selectedIndex];

    const [scriptDefinitionRequested, setScriptDefinitionRequested] = useState(false);
    const asyncTaskDefinition = useEtlScriptDefinition(task, scriptDefinitionRequested);
    const requestScriptDefinitionLoad = () => setScriptDefinitionRequested(true);

    return (
        <ProgressDetailsSheetShell
            className="etl-progress-details-sheet"
            title={task.shared.taskName ?? TaskUtils.studioTaskTypeToDisplay(task.shared.taskType).label}
            locations={allNodes.map((n) => n.location)}
            selectedIndex={selectedIndex}
            direction={direction}
            onNodeChange={handleNodeChange}
        >
            <EtlProgressBody
                task={task}
                nodeInfo={nodeInfo}
                asyncTaskDefinition={asyncTaskDefinition}
                onRequestScriptDefinitionLoad={requestScriptDefinitionLoad}
            />
        </ProgressDetailsSheetShell>
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

function extractErrorMessage(error: Error): string {
    const responseText = (error as { responseText?: string }).responseText;
    if (!responseText) {
        return error.message;
    }

    const errorAndMessage = recentError.tryExtractMessageAndException(responseText);
    return errorAndMessage.message + (errorAndMessage.error ? ": " + errorAndMessage.error : "");
}

function DetailRow({ label, children }: { label: string; children: React.ReactNode }) {
    return (
        <div className="detail-row">
            <span className="small detail-label">{label}</span>
            <span className="text-truncate">{children}</span>
        </div>
    );
}

interface ScriptPreviewProps {
    task: AnyEtlOngoingTaskInfo;
    transformationName: string;
    asyncTaskDefinition: ReturnType<typeof useEtlScriptDefinition>;
    onRequestLoad: () => void;
}

function ScriptPreview({ task, transformationName, asyncTaskDefinition, onRequestLoad }: ScriptPreviewProps) {
    const { value: isOpen, toggle } = useBoolean(false);

    const handleToggle = () => {
        toggle();
        onRequestLoad();
    };

    const transform = asyncTaskDefinition.result?.Configuration?.Transforms?.find((x) => x.Name === transformationName);

    return (
        <div>
            <div className="d-flex justify-content-between align-items-center">
                <h5 className="mb-0">Script preview</h5>
                <Button variant="link" size="xs" className="p-0" onClick={handleToggle}>
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
                        ) : asyncTaskDefinition.error ? (
                            <EtlError error={extractErrorMessage(asyncTaskDefinition.error)} />
                        ) : (
                            <>
                                <DetailRow label="Task type">
                                    <span className="d-flex align-items-center gap-1">
                                        <Icon
                                            icon={TaskUtils.studioTaskTypeToDisplay(task.shared.taskType).icon}
                                            margin="m-0"
                                        />
                                        {TaskUtils.studioTaskTypeToDisplay(task.shared.taskType).label}
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
                                    <div className="small detail-label">Transform script</div>
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

interface ScriptBodyProps {
    task: AnyEtlOngoingTaskInfo;
    scriptProgress: OngoingTaskNodeEtlProgressDetails;
    asyncTaskDefinition: ReturnType<typeof useEtlScriptDefinition>;
    onRequestLoad: () => void;
}

function ScriptBody({ task, scriptProgress, asyncTaskDefinition, onRequestLoad }: ScriptBodyProps) {
    return (
        <>
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
                                    scriptProgress.transactionalId,
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
                <NamedProgressItem progress={scriptProgress.documentTombstones}>tombstones</NamedProgressItem>
                {scriptProgress.counterGroups.total > 0 && (
                    <NamedProgressItem progress={scriptProgress.counterGroups}>counters</NamedProgressItem>
                )}
            </NamedProgress>
            <hr className="script-separator" />
            <ScriptPreview
                task={task}
                transformationName={scriptProgress.transformationName}
                asyncTaskDefinition={asyncTaskDefinition}
                onRequestLoad={onRequestLoad}
            />
        </>
    );
}

interface ScriptSectionProps {
    task: AnyEtlOngoingTaskInfo;
    scriptProgress: OngoingTaskNodeEtlProgressDetails;
    asyncTaskDefinition: ReturnType<typeof useEtlScriptDefinition>;
    onRequestLoad: () => void;
}

function ScriptSection({ task, scriptProgress, asyncTaskDefinition, onRequestLoad }: ScriptSectionProps) {
    const { value: isExpanded, toggle: toggleExpanded } = useBoolean(true);

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
                        <ScriptBody
                            task={task}
                            scriptProgress={scriptProgress}
                            asyncTaskDefinition={asyncTaskDefinition}
                            onRequestLoad={onRequestLoad}
                        />
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
}

function EtlError({ error }: { error: string }) {
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

interface EtlProgressBodyProps {
    task: AnyEtlOngoingTaskInfo;
    nodeInfo: OngoingEtlTaskNodeInfo;
    asyncTaskDefinition: ReturnType<typeof useEtlScriptDefinition>;
    onRequestScriptDefinitionLoad: () => void;
}

function EtlProgressBody({ task, nodeInfo, asyncTaskDefinition, onRequestScriptDefinitionLoad }: EtlProgressBodyProps) {
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
                    <ScriptBody
                        task={task}
                        scriptProgress={progress[0]}
                        asyncTaskDefinition={asyncTaskDefinition}
                        onRequestLoad={onRequestScriptDefinitionLoad}
                    />
                </div>
            ) : (
                <div>
                    {progress.map((scriptProgress) => (
                        <ScriptSection
                            key={scriptProgress.transformationName}
                            task={task}
                            scriptProgress={scriptProgress}
                            asyncTaskDefinition={asyncTaskDefinition}
                            onRequestLoad={onRequestScriptDefinitionLoad}
                        />
                    ))}
                </div>
            )}
        </div>
    );
}
