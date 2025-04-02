import React from "react";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
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
import useDialog from "components/common/Dialog";
import Code from "components/common/Code";
import ClusterSnapshotInstallation from "components/pages/resources/manageServer/advanced/clusterDebug/partials/ClusterSnapshotInstallation";
import SizeGetter from "components/common/SizeGetter";
import Table from "react-bootstrap/Table";
import Badge from "react-bootstrap/Badge";
import Button from "react-bootstrap/Button";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import ProgressBar from "react-bootstrap/ProgressBar";
import { EmptySet } from "components/common/EmptySet";

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

    const dialog = useDialog();
    const hasAnyCriticalError = nodes.some((x) => !!x.data?.criticalError);

    const openInstallationDetails = async (nodeTag: string) => {
        const nodeData = nodes.find((x) => x.nodeTag === nodeTag);

        if (nodeData.data.installingSnapshot) {
            await dialog({
                title: "Cluster Snapshot installation progress for node: " + nodeTag,
                modalSize: "lg",
                message: (
                    <SizeGetter
                        render={(size) => (
                            <ClusterSnapshotInstallation
                                availableWidth={size.width}
                                messages={nodeData.data.installationLog}
                            />
                        )}
                    />
                ),
            });
        }
    };

    const showConnectionDetails = async (connection: Raven.Server.Rachis.RaftDebugView.PeerConnection) => {
        const jsonString = JSON.stringify(connection, null, 4);
        await dialog({
            title: "Connection details",
            message: <Code elementToCopy={jsonString} code={jsonString} language="json" />,
            modalSize: "lg",
        });
    };

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
        <React.Fragment key="summary">
            <Table variant="dark" bordered responsive className="mb-1 rounded-1 overflow-hidden cluster-debug-summary">
                <thead>
                    <tr>
                        <th></th>
                        {nodes.map((node) => {
                            const nodeInfo = allNodes.find((x) => x.nodeTag === node.nodeTag);
                            return (
                                <th key={node.nodeTag}>
                                    <div className="d-flex gap-1 align-items-center">
                                        <span>
                                            <Icon
                                                icon={node.data?.role === "Leader" ? "node-leader" : "cluster-member"}
                                                color="node"
                                            />
                                            <span className="text-nowrap">Node {node.nodeTag}</span>
                                        </span>
                                        {localNode?.nodeTag === node.nodeTag && (
                                            <Badge bg="node" pill>
                                                Current
                                            </Badge>
                                        )}
                                        <FlexGrow />
                                        <a
                                            target="_blank"
                                            href={jsonUrl(nodeInfo?.serverUrl)}
                                            title="See the raw Raft log info and entries (JSON)"
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
                        <th>
                            Role / Phase
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        <ul>
                                            <li>
                                                The node&apos;s role:
                                                <br />(<i>Leader, Follower, Candidate, or Passive</i>).
                                            </li>
                                            <li>
                                                Followers also indicate their current phase:
                                                <br />(<i>Initial, Negotiation, Snapshot, or Steady</i>).
                                            </li>
                                        </ul>
                                    </>
                                }
                            >
                                <Icon id="ReplicationInfo" icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
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
                                                        variant="secondary"
                                                        size="xs"
                                                        onClick={() => openInstallationDetails(node.nodeTag)}
                                                    >
                                                        <Icon icon="preview" />
                                                        View details
                                                    </Button>
                                                </div>
                                            );
                                        } else {
                                            return <>{node.data.role}</>;
                                        }
                                    }}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Progress
                            <PopoverWithHoverWrapper message="Percentage of Raft commands committed on the node out of the total in the log.">
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            if (node.status === "failure") {
                                return (
                                    <td
                                        rowSpan={hasAnyCriticalError ? 9 : 8}
                                        className="align-content-center"
                                        key={node.nodeTag}
                                    >
                                        <EmptySet color="danger" icon="warning">
                                            <span>Unable to connect</span>
                                            <br />
                                            <small className="text-muted">
                                                There was connection issue with{" "}
                                                <Icon icon="node" addon="warning" margin="ms-1 me-1" /> {node.nodeTag}
                                            </small>
                                        </EmptySet>
                                    </td>
                                );
                            }

                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => (
                                        <PopoverWithHoverWrapper
                                            inline={false}
                                            message={
                                                <>
                                                    First entry index:{" "}
                                                    <strong>{node.data.firstEntryIndex.toLocaleString()}</strong>
                                                    <br />
                                                    Last commit index:{" "}
                                                    <strong>{node.data.commitIndex.toLocaleString()}</strong>
                                                    <br />
                                                    Last log entry index:{" "}
                                                    <strong>{node.data.lastLogEntryIndex.toLocaleString()}</strong>
                                                    <br />
                                                </>
                                            }
                                        >
                                            <ProgressBar
                                                variant={node.data.progress === 100 ? "success" : "progress"}
                                                striped={node.data.progress < 100}
                                                now={node.data.progress}
                                                animated={node.data.progress < 100}
                                                label={`${node.data.progress}%`}
                                            />
                                        </PopoverWithHoverWrapper>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Queue size
                            <PopoverWithHoverWrapper message="Number of Raft commands left to be committed on the node.">
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => <>{node.data.queueSize.toLocaleString()}</>}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Last commit index
                            <PopoverWithHoverWrapper message="The index of the last Raft command that was committed on the node.">
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <ConditionalRender
                                    node={node}
                                    key={node.nodeTag}
                                    tdClassName={classNames(node.data?.chocked && "bg-faded-warning text-warning")}
                                >
                                    {() => (
                                        <>
                                            {node.data.commitIndex.toLocaleString()}
                                            {node.data.chocked && (
                                                <PopoverWithHoverWrapper
                                                    message={
                                                        <>
                                                            <span className="text-warning">
                                                                <Icon icon="warning" />
                                                                Warning:
                                                            </span>
                                                            <span> No commits for over 2 minutes</span>
                                                        </>
                                                    }
                                                >
                                                    <Icon icon="warning" margin="ms-1" />
                                                </PopoverWithHoverWrapper>
                                            )}
                                        </>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Last committed date{" "}
                            <PopoverWithHoverWrapper message="The time the last Raft command was committed on the node.">
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            const lastCommitedAsAgo = node.data?.lastCommitedTime
                                ? genUtils.formatDurationByDate(moment.utc(node.data.lastCommitedTime), true)
                                : null;
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => (
                                        <PopoverWithHoverWrapper
                                            message={lastCommitedAsAgo ? <> {node.data.lastCommitedTime}</> : null}
                                        >
                                            <div>{lastCommitedAsAgo ?? "n/a"}</div>
                                        </PopoverWithHoverWrapper>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Last append date
                            <PopoverWithHoverWrapper message="The time the last command was appended to the Raft log.">
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            const lastAppendedAsAgo = node.data?.lastAppendedTime
                                ? genUtils.formatDurationByDate(moment.utc(node.data.lastAppendedTime), true)
                                : null;
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => (
                                        <PopoverWithHoverWrapper
                                            message={lastAppendedAsAgo ? node.data.lastAppendedTime : null}
                                        >
                                            <div id={"last-appended-" + node.nodeTag}>{lastAppendedAsAgo ?? "n/a"}</div>
                                        </PopoverWithHoverWrapper>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    <tr>
                        <th>
                            Local version
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        <ul>
                                            <li>
                                                Each Raft command has an ID number associated with it (not the log
                                                index). Newer RavenDB versions may introduce commands with higher
                                                version numbers that are unknown to nodes running older versions.
                                            </li>
                                            <li>
                                                This value shows the highest Raft command version number known to the
                                                node.
                                            </li>
                                        </ul>
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" id="localVersionTooltip" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => (
                            <ConditionalRender node={node} key={node.nodeTag}>
                                {() => <>{node.data.localVersion}</>}
                            </ConditionalRender>
                        ))}
                    </tr>
                    <tr>
                        <th>
                            Connection
                            <PopoverWithHoverWrapper message="The node's connection state to other nodes in the cluster.">
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </th>
                        {nodes.map((node) => {
                            return (
                                <ConditionalRender node={node} key={node.nodeTag}>
                                    {() => (
                                        <div>
                                            {node.data.connections.map((connection) => (
                                                <Button
                                                    title="Click to see connection details"
                                                    size="xs"
                                                    key={connection.Destination}
                                                    onClick={() => showConnectionDetails(connection)}
                                                    variant={connection.Connected ? "success" : "danger"}
                                                    className="me-1 rounded-pill"
                                                >
                                                    <Icon icon={connection.Connected ? "connected" : "disconnected"} />
                                                    {connection.Destination}
                                                </Button>
                                            ))}
                                        </div>
                                    )}
                                </ConditionalRender>
                            );
                        })}
                    </tr>
                    {hasAnyCriticalError && (
                        <tr>
                            <th>Cluster Critical Error</th>
                            {nodes.map((node) => {
                                return (
                                    <ConditionalRender node={node} key={node.nodeTag}>
                                        {() => (
                                            <div>
                                                {node.data.criticalError ? (
                                                    <Button
                                                        size="xs"
                                                        variant="danger"
                                                        onClick={() => openCriticalError(node.nodeTag)}
                                                    >
                                                        <Icon icon="preview" />
                                                        View error
                                                    </Button>
                                                ) : (
                                                    <>-</>
                                                )}
                                            </div>
                                        )}
                                    </ConditionalRender>
                                );
                            })}
                        </tr>
                    )}
                </tbody>
            </Table>
        </React.Fragment>
    );
}

interface ConditionalRenderProps {
    node: nodeAwareLoadableData<ClusterDebugNodeInfo>;
    children: () => React.ReactNode;
    tdClassName?: string;
}

function ConditionalRender(props: ConditionalRenderProps) {
    const { node, children, tdClassName } = props;
    const status = node.status;
    switch (status) {
        case "loading":
            return (
                <td className={tdClassName}>
                    <LazyLoad active={true}>
                        <div>&nbsp;</div>
                    </LazyLoad>
                </td>
            );
        case "idle":
        case "failure":
            return null;
        case "success":
            return <td className={classNames("align-content-center", tdClassName)}>{children()}</td>;
        default:
            assertUnreachable(status);
    }
}
