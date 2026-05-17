import React from "react";
import { OngoingTaskReplicationHubInfo } from "components/models/tasks";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
} from "components/common/RichPanel";
import { DestinationUrlItem, OngoingTaskName, OngoingTaskResponsibleNode } from "../../shared/shared";
import { ExternalReplicationTaskDistribution } from "../partials/ExternalReplicationTaskDistribution";
import { Icon } from "components/common/Icon";

interface ReplicationHubConnectedSinkPanelProps {
    data: OngoingTaskReplicationHubInfo;
}

export function ReplicationHubConnectedSinkPanel(props: ReplicationHubConnectedSinkPanelProps) {
    const { data } = props;

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    <OngoingTaskName task={data} canEdit={false} editUrl={undefined} />
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
                <RichPanelDetailItem label="Task Name">{data.shared.taskName}</RichPanelDetailItem>
                <RichPanelDetailItem label="Sink Database">{data.shared.destinationDatabase}</RichPanelDetailItem>
                <DestinationUrlItem destinationUrl={data.shared.destinationUrl} label="Actual Sink URL" />
            </RichPanelDetails>
            <ExternalReplicationTaskDistribution task={data} />
        </RichPanel>
    );
}
