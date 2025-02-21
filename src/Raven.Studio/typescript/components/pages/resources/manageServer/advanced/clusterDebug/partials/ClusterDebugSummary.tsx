import React from "react";
import { Badge, Button, Table, UncontrolledTooltip } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import ProgressBarWithTrackingPoint from "components/common/ProgressBarWithTrackingPoint";
import "./ClusterDebugSummary.scss";
import classNames from "classnames";
import { nodeAwareLoadableData } from "hooks/useClusterWideAsync";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import endpoints from "endpoints";
import { LazyLoad } from "components/common/LazyLoad";
import assertUnreachable from "components/utils/assertUnreachable";
import { ClusterDebugNodeInfo } from "components/pages/resources/manageServer/advanced/clusterDebug/partials/common";
import genUtils from "common/generalUtils";
import moment from "moment";
import notificationCenter from "common/notifications/notificationCenter";
import messagePublisher from "common/messagePublisher";
import { withPreventDefault } from "components/utils/common";

interface ClusterDebugSummaryProps {
    nodes: nodeAwareLoadableData<ClusterDebugNodeInfo>[];
}

function jsonUrl(serverUrl: string) {
    return serverUrl + endpoints.global.rachisAdmin.adminClusterLog;
}

export default function ClusterDebugSummary(props: ClusterDebugSummaryProps) {
    const { nodes } = props;

    const localNode = useAppSelector(clusterSelectors.localNode);
    const allNodes = useAppSelector(clusterSelectors.allNodes);

    const hasAnyCriticalError = nodes.some((x) => !!x.data?.criticalError);

    const openInstallationDetails = (nodeTag: string) => {};

    const showConnectionDetails = (connection: Raven.Server.Rachis.RaftDebugView.PeerConnection) => {};

    const openCriticalError = (nodeTag: string) => {
        if (nodeTag !== localNode.nodeTag) {
            messagePublisher.reportInfo("Please go to node " + nodeTag + " to see an error details");
            return;
        }

        const nodeData = nodes.find((x) => x.nodeTag === nodeTag);

        const alertId = nodeData.data.criticalError.Id;
        const criticalErrorAlert = notificationCenter.instance.globalNotifications().find((x) => x.id === alertId);
        if (!criticalErrorAlert) {
            messagePublisher.reportError("Unable to find critical error alert");
        }

        notificationCenter.instance.openDetails(criticalErrorAlert);
    };

    return (
        <>
            <Table dark bordered responsive className="mb-1 rounded-1 overflow-hidden">
                <thead>
                    <tr>
                        <th></th>
                        {nodes.map((node) => {
                            const nodeInfo = allNodes.find((x) => x.nodeTag === node.nodeTag);
                            return (
                                <th key={node.nodeTag}>
                                    <div className="d-flex gap-1 align-items-center">
                                        <span>
                                            {node.data?.role === "Leader" ? (
                                                <Icon icon="node-leader" color="node" />
                                            ) : (
                                                <Icon icon="cluster-member" />
                                            )}
                                            <span className="text-nowrap">{node.nodeTag}</span>
                                        </span>
                                        {localNode?.nodeTag === node.nodeTag && (
                                            <Badge color="node" pill>
                                                Current
                                            </Badge>
                                        )}
                                        <FlexGrow />
                                        <a
                                            target="_blank"
                                            href={jsonUrl(nodeInfo?.serverUrl)}
                                            title="See json"
                                            className="no-decor"
                                        >
                                            <Icon icon="json" margin="m-0" />
                                        </a>
                                    </div>
                                </th>
                            );
                        })}
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <th>Progress</th>
                        {nodes.map((node) => {
                            return (
                                <td key={node.nodeTag}>
                                    <ConditionalRender node={node}>
                                        {() => {
                                            return (
                                                <>
                                                    <div className="hstack gap-1" id={"progress-" + node.nodeTag}>
                                                        <span className="text-nowrap">{node.data.progress}%</span>
                                                        <ProgressBarWithTrackingPoint
                                                            startingPoint={0}
                                                            progress={node.data.progress}
                                                            endingPoint={100}
                                                        />
                                                    </div>
                                                    <UncontrolledTooltip target={"progress-" + node.nodeTag}>
                                                        First entry index:{" "}
                                                        <strong>{node.data.firstEntryIndex.toLocaleString()}</strong>
                                                        <br />
                                                        Commit index:{" "}
                                                        <strong>{node.data.commitIndex.toLocaleString()}</strong>
                                                        <br />
                                                        Last log entry index:{" "}
                                                        <strong>{node.data.lastLogEntryIndex.toLocaleString()}</strong>
                                                        <br />
                                                    </UncontrolledTooltip>
                                                </>
                                            );
                                        }}
                                    </ConditionalRender>
                                </td>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Queue size <Icon icon="info" color="info" margin="ms-1" id="queueSizeTooltip" />
                            <UncontrolledTooltip target="queueSizeTooltip" placement="right">
                                This is text for Queue size tooltip
                            </UncontrolledTooltip>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <td key={node.nodeTag}>
                                    <ConditionalRender node={node}>
                                        {() => <>{node.data.queueSize.toLocaleString()}</>}
                                    </ConditionalRender>
                                </td>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Commit index <Icon icon="info" color="info" margin="ms-1" id="commitIndexTooltip" />
                            <UncontrolledTooltip target="commitIndexTooltip" placement="right">
                                This is text for Commit index tooltip
                            </UncontrolledTooltip>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <td
                                    key={node.nodeTag}
                                    className={classNames(node.data?.chocked && "bg-faded-warning text-warning")}
                                >
                                    <ConditionalRender node={node}>
                                        {() => (
                                            <>
                                                {node.data.commitIndex.toLocaleString()}
                                                {node.data.chocked && (
                                                    <>
                                                        <Icon
                                                            icon="warning"
                                                            margin="ms-1"
                                                            id={"failed-commit-" + node.nodeTag}
                                                        />
                                                        <UncontrolledTooltip target={"failed-commit-" + node.nodeTag}>
                                                            <span className="text-warning">
                                                                Warning: No commits for over 2 minutes
                                                            </span>
                                                        </UncontrolledTooltip>
                                                    </>
                                                )}
                                            </>
                                        )}
                                    </ConditionalRender>
                                </td>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Role / Phase <Icon icon="info" color="info" margin="ms-1" id="phaseTooltip" />
                            <UncontrolledTooltip target="phaseTooltip" placement="right">
                                This is text for phase tooltip
                            </UncontrolledTooltip>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <td key={node.nodeTag}>
                                    <ConditionalRender node={node}>
                                        {() => {
                                            if (node.data.installingSnapshot) {
                                                return (
                                                    <div className="d-flex">
                                                        {node.data.role} / Installing Snapshot{" "}
                                                        <small className="ms-1">
                                                            <span className="global-spinner spinner-sm"></span>
                                                        </small>
                                                        <FlexGrow />
                                                        <Button
                                                            size="sm"
                                                            onClick={() => openInstallationDetails(node.nodeTag)}
                                                        >
                                                            view details
                                                        </Button>
                                                    </div>
                                                );
                                            } else {
                                                return <>{node.data.role}</>;
                                            }
                                        }}
                                    </ConditionalRender>
                                </td>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Last committed date{" "}
                            <Icon icon="info" color="info" margin="ms-1" id="lastCommittedDateTooltip" />
                            <UncontrolledTooltip target="lastCommittedDateTooltip" placement="right">
                                This is text for Last committed date tooltip
                            </UncontrolledTooltip>
                        </th>
                        {nodes.map((node) => {
                            const lastCommitedAsAgo = node.data?.lastCommitedTime
                                ? genUtils.formatDurationByDate(moment.utc(node.data.lastCommitedTime), true)
                                : null;
                            return (
                                <td key={node.nodeTag}>
                                    <ConditionalRender node={node}>
                                        {() => (
                                            <>
                                                <div id={"last-commited-" + node.nodeTag}>
                                                    {lastCommitedAsAgo ?? "n/a"}
                                                </div>
                                                {lastCommitedAsAgo && (
                                                    <UncontrolledTooltip target={"last-commited-" + node.nodeTag}>
                                                        {node.data.lastCommitedTime}
                                                    </UncontrolledTooltip>
                                                )}
                                            </>
                                        )}
                                    </ConditionalRender>
                                </td>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Last append date <Icon icon="info" color="info" margin="ms-1" id="lastAppendDateTooltip" />
                            <UncontrolledTooltip target="lastAppendDateTooltip" placement="right">
                                This is text for Last append date tooltip
                            </UncontrolledTooltip>
                        </th>
                        {nodes.map((node) => {
                            const lastAppendedAsAgo = node.data?.lastAppendedTime
                                ? genUtils.formatDurationByDate(moment.utc(node.data.lastAppendedTime), true)
                                : null;
                            return (
                                <td key={node.nodeTag}>
                                    <ConditionalRender node={node}>
                                        {() => (
                                            <>
                                                <div id={"last-appended-" + node.nodeTag}>
                                                    {lastAppendedAsAgo ?? "n/a"}
                                                </div>
                                                {lastAppendedAsAgo && (
                                                    <UncontrolledTooltip target={"last-appended-" + node.nodeTag}>
                                                        {node.data.lastAppendedTime}
                                                    </UncontrolledTooltip>
                                                )}
                                            </>
                                        )}
                                    </ConditionalRender>
                                </td>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Local version <Icon icon="info" color="info" margin="ms-1" id="localVersionTooltip" />
                            <UncontrolledTooltip target="localVersionTooltip" placement="right">
                                This is text for Local version tooltip
                            </UncontrolledTooltip>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <td key={node.nodeTag}>
                                    <ConditionalRender node={node}>
                                        {() => <>{node.data.localVersion}</>}
                                    </ConditionalRender>
                                </td>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>Connection</th>
                        {nodes.map((node) => {
                            return (
                                <td key={node.nodeTag}>
                                    <ConditionalRender node={node}>
                                        {() => (
                                            <div>
                                                {node.data.connections.map((connection) => (
                                                    <a
                                                        href="#"
                                                        onClick={withPreventDefault(() =>
                                                            showConnectionDetails(connection)
                                                        )}
                                                    >
                                                        <small className="margin-right-sm">
                                                            <strong>
                                                                {connection.Destination}
                                                                {connection.Connected ? (
                                                                    <i className="icon-check text-success"></i>
                                                                ) : (
                                                                    <i className="icon-danger text-danger"></i>
                                                                )}
                                                            </strong>
                                                        </small>
                                                    </a>
                                                ))}
                                            </div>
                                        )}
                                    </ConditionalRender>
                                </td>
                            );
                        })}
                    </tr>
                    {hasAnyCriticalError && (
                        <tr>
                            <th>Cluster Critical Error</th>
                            {nodes.map((node) => {
                                return (
                                    <td key={node.nodeTag}>
                                        <ConditionalRender node={node}>
                                            {() => (
                                                <div>
                                                    {node.data.criticalError ? (
                                                        <Button
                                                            size="sm"
                                                            color="danger"
                                                            onClick={() => openCriticalError(node.nodeTag)}
                                                        >
                                                            view details
                                                        </Button>
                                                    ) : (
                                                        <>-</>
                                                    )}
                                                </div>
                                            )}
                                        </ConditionalRender>
                                    </td>
                                );
                            })}
                        </tr>
                    )}
                </tbody>
            </Table>
        </>
    );
}

interface ConditionalRenderProps {
    node: nodeAwareLoadableData<ClusterDebugNodeInfo>;
    children: () => React.ReactNode;
}

function ConditionalRender(props: ConditionalRenderProps) {
    const { node, children } = props;
    const status = node.status;
    switch (status) {
        case "loading":
            return (
                <LazyLoad active={true}>
                    <div>&nbsp;</div>
                </LazyLoad>
            );
        case "idle":
        case "failure":
            return null;
        case "success":
            return <React.Fragment>{children()}</React.Fragment>;
        default:
            assertUnreachable(status);
    }
}
