import React from "react";
import { OngoingTaskReplicationHubInfo } from "../../../../../models/tasks";
import database from "models/resources/database";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import { OngoingTaskName } from "../shared";
import { useAccessManager } from "hooks/useAccessManager";

interface ReplicationHubPanelProps {
    db: database;
    data: OngoingTaskReplicationHubInfo;
}

export function ReplicationHubPanel(props: ReplicationHubPanelProps) {
    const { data, db } = props;
    const { isAdminAccessOrAbove } = useAccessManager();

    //TODO: responsible node
    //TODO: task status (sink)

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
    //TODO
    return (
        <div className="panel destination-item pull-replication-hub">
            <div className="collapse panel-addon" data-bind="collapse: showDetails">
                <div className="padding-sm flex-horizontal" data-bind="visible: showDelayReplication">
                    <div className="flex-grow">
                        <div className="list-properties">
                            <div className="property-item" data-bind="visible: showDelayReplication">
                                <div className="property-name">Replication Delay Time:</div>
                                <div
                                    className="property-value"
                                    data-bind="text: delayHumane"
                                    title="Replication Delay Time"
                                ></div>
                            </div>
                        </div>
                    </div>
                </div>
                <div className="padding-sm" data-bind="visible: ongoingHubs().length">
                    <div data-bind="foreach: ongoingHubs">
                        <div className="panel destination-item external-replication">
                            <div className="panel-addon">
                                <div className="padding-sm">
                                    <div className="inline-properties">
                                        <div className="property-item">
                                            <div className="property-name">Task Status:</div>
                                            <div className="property-value" data-bind="text: badgeText"></div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
