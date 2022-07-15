import React from "react";
import { OngoingTaskReplicationHubInfo } from "../../../../../models/tasks";
import database from "models/resources/database";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import { OngoingTaskName } from "../shared";

interface ReplicationHubPanelProps {
    db: database;
    data: OngoingTaskReplicationHubInfo;
}

export function ReplicationHubPanel(props: ReplicationHubPanelProps) {
    const { data } = props;

    return (
        <RichPanel>
            <RichPanelHeader>
                <OngoingTaskName task={data} canEdit={false} editUrl={undefined} />
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
