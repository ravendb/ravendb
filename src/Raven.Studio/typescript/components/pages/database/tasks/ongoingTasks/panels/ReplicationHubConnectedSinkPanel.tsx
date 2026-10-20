import React from "react";
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
import { ExternalReplicationTaskDistribution } from "../partials/ExternalReplicationTaskDistribution";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/Collapse";

interface ReplicationHubConnectedSinkPanelProps {
    data: OngoingTaskReplicationHubInfo;
    isExpanded: boolean;
    toggleExpanded: () => void;
}

export function ReplicationHubConnectedSinkPanel(props: ReplicationHubConnectedSinkPanelProps) {
    const { data, isExpanded, toggleExpanded } = props;

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
            </RichPanelDetails>
            <Collapse in={isExpanded}>
                <div>
                    <ExternalReplicationTaskDistribution task={data} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
