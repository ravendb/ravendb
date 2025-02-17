import React from "react";
import { Badge, Table, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import ProgressBarWithTrackingPoint from "components/common/ProgressBarWithTrackingPoint";
import "./ClusterDebugSummary.scss";
import classNames from "classnames";
import useUniqueId from "hooks/useUniqueId";

interface NodeData {
    name: string;
    current: boolean;
    type: "Leader" | "Member";
    progress: number;
    queueSize: number;
    commitIndex: number;
    hasCommitFailed: boolean;
    phase: string;
    lastCommittedDate: string;
    lastAppendDate: string;
    localVersion: string;
    jsonUrl: string;
}

export const nodeData: NodeData[] = [
    {
        name: "Node A",
        current: true,
        type: "Leader",
        progress: 70,
        queueSize: 4,
        commitIndex: 24,
        hasCommitFailed: true,
        phase: "Snapshot",
        lastCommittedDate: "3 minutes ago",
        lastAppendDate: "3 minutes ago",
        localVersion: "60_000",
        jsonUrl: "#",
    },
    {
        name: "Node B",
        current: false,
        type: "Member",
        progress: 70,
        queueSize: 4,
        commitIndex: 24,
        hasCommitFailed: false,
        phase: "Snapshot",
        lastCommittedDate: "3 minutes ago",
        lastAppendDate: "3 minutes ago",
        localVersion: "60_000",
        jsonUrl: "#",
    },
    {
        name: "Node C",
        current: false,
        type: "Member",
        progress: 70,
        queueSize: 4,
        commitIndex: 24,
        hasCommitFailed: false,
        phase: "Snapshot",
        lastCommittedDate: "3 minutes ago",
        lastAppendDate: "3 minutes ago",
        localVersion: "60_000",
        jsonUrl: "#",
    },
    {
        name: "Node D",
        current: false,
        type: "Member",
        progress: 70,
        queueSize: 4,
        commitIndex: 24,
        hasCommitFailed: false,
        phase: "Snapshot",
        lastCommittedDate: "3 minutes ago",
        lastAppendDate: "3 minutes ago",
        localVersion: "60_000",
        jsonUrl: "#",
    },
    {
        name: "Node E",
        current: false,
        type: "Member",
        progress: 70,
        queueSize: 4,
        commitIndex: 24,
        hasCommitFailed: false,
        phase: "Snapshot",
        lastCommittedDate: "3 minutes ago",
        lastAppendDate: "3 minutes ago",
        localVersion: "60_000",
        jsonUrl: "#",
    },
];

const showNodeIcon = (type: "Leader" | "Member") => {
    return type === "Leader" ? <Icon icon="node-leader" color="node" /> : <Icon icon="cluster-member" />;
};

const nodeColumnHeader = (node: NodeData) => {
    return (
        <div className="d-flex gap-1 align-items-center">
            <span>
                {showNodeIcon(node.type)}
                <span className="text-nowrap">{node.name}</span>
            </span>
            {node.current && (
                <Badge color="node" pill>
                    Current
                </Badge>
            )}
            <FlexGrow />
            <a href={node.jsonUrl} title="See json" className="no-decor">
                <Icon icon="json" margin="m-0" />
            </a>
        </div>
    );
};

export default function ClusterDebugSummary() {
    const failedCommitTooltipId = useUniqueId("failedCommitTooltip");
    return (
        <>
            <Table dark bordered responsive className="mb-1 rounded-1 overflow-hidden">
                <thead>
                    <tr>
                        <th></th>
                        {nodeData.map((node) => (
                            <th key={node.name}>{nodeColumnHeader(node)}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <th>Progress</th>
                        {nodeData.map((node) => (
                            <td key={`${node.name}-progress`}>
                                <div className="hstack gap-1">
                                    <span className="text-nowrap">{node.progress}%</span>
                                    <ProgressBarWithTrackingPoint
                                        startingPoint={0}
                                        progress={node.progress}
                                        endingPoint={100}
                                    />
                                </div>
                            </td>
                        ))}
                    </tr>
                    <tr>
                        <th>
                            Queue size <Icon icon="info" color="info" margin="ms-1" id="queueSizeTooltip" />
                        </th>
                        {nodeData.map((node) => (
                            <td key={`${node.name}-queue`}>{node.queueSize}</td>
                        ))}
                    </tr>
                    <tr>
                        <th>
                            Commit index <Icon icon="info" color="info" margin="ms-1" id="commitIndexTooltip" />
                        </th>
                        {nodeData.map((node) => (
                            <td
                                key={`${node.name}-commit`}
                                className={classNames(node.hasCommitFailed && "bg-faded-warning text-warning")}
                            >
                                {node.commitIndex}
                                {node.hasCommitFailed && (
                                    <>
                                        <Icon icon="warning" margin="ms-1" id={failedCommitTooltipId} />
                                        <UncontrolledTooltip target={failedCommitTooltipId}>
                                            The commit has failed for unknown reason
                                        </UncontrolledTooltip>
                                    </>
                                )}
                            </td>
                        ))}
                    </tr>
                    <tr>
                        <th>
                            Phase <Icon icon="info" color="info" margin="ms-1" id="phaseTooltip" />
                        </th>
                        {nodeData.map((node) => (
                            <td key={`${node.name}-phase`}>{node.phase}</td>
                        ))}
                    </tr>
                    <tr>
                        <th>
                            Last committed date{" "}
                            <Icon icon="info" color="info" margin="ms-1" id="lastCommittedDateTooltip" />
                        </th>
                        {nodeData.map((node) => (
                            <td key={`${node.name}-lastCommit`}>{node.lastCommittedDate}</td>
                        ))}
                    </tr>
                    <tr>
                        <th>
                            Last append date <Icon icon="info" color="info" margin="ms-1" id="lastAppendDateTooltip" />
                        </th>
                        {nodeData.map((node) => (
                            <td key={`${node.name}-lastAppend`}>{node.lastAppendDate}</td>
                        ))}
                    </tr>
                    <tr>
                        <th>
                            Local version <Icon icon="info" color="info" margin="ms-1" id="localVersionTooltip" />
                        </th>
                        {nodeData.map((node) => (
                            <td key={`${node.name}-version`}>{node.localVersion}</td>
                        ))}
                    </tr>
                </tbody>
            </Table>
            <QueueSizeTooltip />
            <CommitIndexTooltip />
            <PhaseTooltip />
            <LastCommittedDateTooltip />
            <LastAppendDateTooltip />
            <LocalVersionTooltip />
        </>
    );
}

export function QueueSizeTooltip() {
    return (
        <UncontrolledTooltip target="queueSizeTooltip" placement="right">
            This is text for Queue size tooltip
        </UncontrolledTooltip>
    );
}

export function CommitIndexTooltip() {
    return (
        <UncontrolledTooltip target="commitIndexTooltip" placement="right">
            This is text for Commit index tooltip
        </UncontrolledTooltip>
    );
}

export function PhaseTooltip() {
    return (
        <UncontrolledTooltip target="phaseTooltip" placement="right">
            This is text for phase tooltip
        </UncontrolledTooltip>
    );
}

export function LastCommittedDateTooltip() {
    return (
        <UncontrolledTooltip target="lastCommittedDateTooltip" placement="right">
            This is text for Last committed date tooltip
        </UncontrolledTooltip>
    );
}

export function LastAppendDateTooltip() {
    return (
        <UncontrolledTooltip target="lastAppendDateTooltip" placement="right">
            This is text for Last append date tooltip
        </UncontrolledTooltip>
    );
}

export function LocalVersionTooltip() {
    return (
        <UncontrolledTooltip target="localVersionTooltip" placement="right">
            This is text for Local version tooltip
        </UncontrolledTooltip>
    );
}
