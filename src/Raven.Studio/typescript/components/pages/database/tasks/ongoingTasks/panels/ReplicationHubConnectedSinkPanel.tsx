import React, { useState } from "react";
import { OngoingTaskReplicationHubInfo } from "components/models/tasks";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelName,
} from "components/common/RichPanel";
import { DestinationUrlItem, OngoingTaskResponsibleNode } from "../../shared/shared";
import {
    ExternalReplicationTaskDistribution,
    ReplicationTaskSummaryProgress,
} from "../partials/ExternalReplicationTaskDistribution";
import {
    getSinkConnectionStatus,
    getSinkError,
    getVisibleNodes,
    sinkConnectionStatusLabels,
} from "../partials/useReplicationHubSinks";
import { ErrorModal } from "../partials/ErrorModal";
import { Icon } from "components/common/Icon";
import { withPreventDefault } from "components/utils/common";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";

interface ReplicationHubConnectedSinkPanelProps {
    data: OngoingTaskReplicationHubInfo;
    isExpanded: boolean;
    toggleExpanded: () => void;
}

export function ReplicationHubConnectedSinkPanel(props: ReplicationHubConnectedSinkPanelProps) {
    const { data, isExpanded, toggleExpanded } = props;

    const visibleNodes = getVisibleNodes(data);
    const connectionStatus = getSinkConnectionStatus(data);
    const error = getSinkError(data);

    return (
        <RichPanel className="well">
            <RichPanelHeader>
                <RichPanelInfo>
                    <RichPanelName size="sm" title={"Sink database: " + data.shared.destinationDatabase}>
                        {data.shared.destinationDatabase}
                    </RichPanelName>
                </RichPanelInfo>
                <RichPanelActions>
                    <span>
                        <Icon icon="pull-replication-agent" />
                        Replication Sink
                    </span>
                    <OngoingTaskResponsibleNode task={data} />
                </RichPanelActions>
            </RichPanelHeader>
            <RichPanelDetails>
                <RichPanelDetailItem>
                    <Button
                        variant="secondary"
                        onClick={toggleExpanded}
                        title={`${isExpanded ? "Collapse" : "Expand"} distribution details for ${data.shared.destinationDatabase}`}
                        className="btn-toggle-panel rounded-pill"
                    >
                        <Icon icon={isExpanded ? "fold" : "unfold"} margin="m-0" />
                    </Button>
                </RichPanelDetailItem>
                <DestinationUrlItem destinationUrl={data.shared.destinationUrl} label="Actual Sink URL" />
                {connectionStatus && (
                    <RichPanelDetailItem label="Replication Status">
                        {sinkConnectionStatusLabels[connectionStatus]}
                    </RichPanelDetailItem>
                )}
                {error && <SinkErrorItem error={error} />}
                {visibleNodes.length > 0 && (
                    <RichPanelDetailItem>
                        <ReplicationTaskSummaryProgress task={data} nodesInfo={visibleNodes} />
                    </RichPanelDetailItem>
                )}
            </RichPanelDetails>
            <Collapse in={isExpanded}>
                <div>
                    <ExternalReplicationTaskDistribution task={data} />
                </div>
            </Collapse>
        </RichPanel>
    );
}

function SinkErrorItem({ error }: { error: string }) {
    const [isErrorModalOpen, setIsErrorModalOpen] = useState(false);

    const toggleErrorModal = () => setIsErrorModalOpen((isOpen) => !isOpen);

    return (
        <RichPanelDetailItem>
            <a
                href="#"
                onClick={withPreventDefault(toggleErrorModal)}
                className="d-flex gap-1 align-items-center"
                title="Show error details"
            >
                <Icon icon="warning" color="danger" margin="m-0" />
                <span className="text-danger">Error</span>
            </a>
            {isErrorModalOpen && <ErrorModal toggleErrorModal={toggleErrorModal} error={error} />}
        </RichPanelDetailItem>
    );
}
