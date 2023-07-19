import React from "react";
import { OngoingTaskReplicationHubInfo } from "components/models/tasks";
import database from "models/resources/database";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
} from "components/common/RichPanel";
import { OngoingTaskName, OngoingTaskResponsibleNode } from "../../shared";

interface ReplicationHubConnectedSinkPanelProps {
    db: database;
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
                    <OngoingTaskResponsibleNode task={data} />
                </RichPanelActions>
            </RichPanelHeader>
            <RichPanelDetails>
                <RichPanelDetailItem label="Task Name">{data.shared.taskName}</RichPanelDetailItem>
                <RichPanelDetailItem label="Sink Database">{data.shared.destinationDatabase}</RichPanelDetailItem>
                {data.shared.destinationUrl && (
                    <RichPanelDetailItem label="Actual Sink URL">
                        <a href={data.shared.destinationUrl} target="_blank">
                            {data.shared.destinationUrl}
                        </a>
                    </RichPanelDetailItem>
                )}
            </RichPanelDetails>
        </RichPanel>
    );
}
