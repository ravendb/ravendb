import React from "react";
import {
    OngoingTaskAzureServiceBusSinkInfo,
    OngoingTaskKafkaSinkInfo,
    OngoingTaskRabbitMqSinkInfo,
} from "components/models/tasks";
import { RichPanelDetailItem, RichPanelDetails } from "components/common/RichPanel";
import { ConnectionStringItem } from "../../shared/shared";
import { StudioConnectionType } from "components/pages/database/settings/connectionStrings/connectionStringsTypes";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import Collapse from "react-bootstrap/Collapse";
import { DetailsToggleProps, TaskPanelToggleButton } from "./EtlPanelComponents";
import { TaskConnectionStatusItem } from "../partials/TaskConnectionStatus";

interface QueueSinkPanelDetailsProps extends DetailsToggleProps {
    task: OngoingTaskKafkaSinkInfo | OngoingTaskRabbitMqSinkInfo | OngoingTaskAzureServiceBusSinkInfo;
    canEdit: boolean;
    connectionStringType: StudioConnectionType;
    queuesLabel: string;
    urlLabel?: string;
}

export function QueueSinkPanelDetails(props: QueueSinkPanelDetailsProps) {
    const { task, canEdit, connectionStringType, queuesLabel, urlLabel, detailsVisible, toggleDetails } = props;
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const queues = task.shared.queues.length > 0 ? task.shared.queues.join(", ") : "No enabled scripts";

    return (
        <>
            <RichPanelDetails>
                <TaskPanelToggleButton detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
                <TaskConnectionStatusItem task={task} label="Connection Status" />
                <RichPanelDetailItem label={queuesLabel} title={queues}>
                    <div className="queue-sink-queues text-truncate">{queues}</div>
                </RichPanelDetailItem>
            </RichPanelDetails>
            <Collapse in={detailsVisible}>
                <div>
                    <RichPanelDetails>
                        {urlLabel && <RichPanelDetailItem label={urlLabel}>{task.shared.url}</RichPanelDetailItem>}
                        <ConnectionStringItem
                            connectionStringDefined
                            canEdit={canEdit}
                            connectionStringName={task.shared.connectionStringName}
                            connectionStringType={connectionStringType}
                            databaseName={databaseName}
                        />
                        <RichPanelDetailItem label="Scripts">
                            {task.shared.enabledScriptsCount.toLocaleString()} /{" "}
                            {task.shared.scriptsCount.toLocaleString()} enabled
                        </RichPanelDetailItem>
                    </RichPanelDetails>
                </div>
            </Collapse>
        </>
    );
}
