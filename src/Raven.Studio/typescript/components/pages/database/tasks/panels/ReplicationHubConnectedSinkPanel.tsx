import React from "react";
import { OngoingTaskReplicationHubInfo } from "../../../../models/tasks";
import database from "models/resources/database";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
} from "../../../../common/RichPanel";
import { OngoingTaskName, OngoingTaskResponsibleNode } from "../shared";

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
                <RichPanelDetailItem>
                    Task Name
                    <div className="value">{data.shared.taskName}</div>
                </RichPanelDetailItem>
                <RichPanelDetailItem>
                    Sink Database:
                    <div className="value">{data.shared.destinationDatabase}</div>
                </RichPanelDetailItem>
                {data.shared.destinationUrl && (
                    <RichPanelDetailItem>
                        Actual Sink URL:
                        <div className="value">
                            <a href={data.shared.destinationUrl} target="_blank">
                                {data.shared.destinationUrl}
                            </a>
                        </div>
                    </RichPanelDetailItem>
                )}
            </RichPanelDetails>
        </RichPanel>
    );
}
