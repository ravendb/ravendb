import React from "react";
import {
    ConnectionStringItem,
    EmptyScriptsWarning,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
} from "../../shared/shared";
import { OngoingTaskKafkaEtlInfo } from "components/models/tasks";
import { useAppUrls } from "hooks/useAppUrls";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import { OngoingEtlTaskDistribution } from "../partials/OngoingEtlTaskDistribution";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { EtlPanelBaseProps, useEtlPanel } from "./etlPanelUtils";
import {
    TaskPanelErrors,
    EtlPanelHealthBadge,
    EtlPanelProgressItem,
    TaskPanelToggleButton,
} from "./EtlPanelComponents";

type KafkaEtlPanelProps = EtlPanelBaseProps<OngoingTaskKafkaEtlInfo>;

export function KafkaEtlPanel(props: KafkaEtlPanelProps) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState, etlStats } = props;

    const { forCurrentDatabase } = useAppUrls();
    const editUrl = forCurrentDatabase.editKafkaEtl(data.shared.taskId)();

    const {
        canEdit,
        goToTaskErrors,
        detailsVisible,
        toggleDetails,
        onEdit,
        taskHealth,
        errorCount,
        errorsByLocation,
        etlProgress,
    } = useEtlPanel(props, editUrl);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {canEdit && (
                        <RichPanelSelect>
                            <Form.Check
                                type="checkbox"
                                onChange={(e) => toggleSelection(e.currentTarget.checked, data.shared)}
                                checked={isSelected(data.shared.taskId)}
                            />
                        </RichPanelSelect>
                    )}
                    <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                </RichPanelInfo>
                <RichPanelActions>
                    <span>
                        <Icon icon="kafka-etl" />
                        Kafka ETL
                    </span>
                    <OngoingTaskResponsibleNode task={data} />
                    <OngoingTaskStatus
                        task={data}
                        canEdit={canEdit}
                        onTaskOperation={onTaskOperation}
                        isTogglingState={isTogglingState(data.shared.taskId)}
                    />
                    <OngoingTaskActions
                        task={data}
                        canEdit={canEdit}
                        onEdit={onEdit}
                        onTaskOperation={onTaskOperation}
                        isDeleting={isDeleting(data.shared.taskId)}
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <RichPanelDetails>
                <TaskPanelToggleButton detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
                <ConnectionStringItem
                    connectionStringDefined
                    canEdit={canEdit}
                    connectionStringName={data.shared.connectionStringName}
                    connectionStringType="Kafka"
                    databaseName={databaseName}
                />
                <RichPanelDetailItem label="Bootstrap Servers">{data.shared.url}</RichPanelDetailItem>
                <EtlPanelHealthBadge taskHealth={taskHealth} />
                <TaskPanelErrors
                    errorCount={errorCount}
                    errorsByLocation={errorsByLocation}
                    goToTaskErrors={goToTaskErrors}
                />
                <EtlPanelProgressItem etlProgress={etlProgress} />
                <EmptyScriptsWarning task={data} />
            </RichPanelDetails>
            <Collapse in={detailsVisible}>
                <div>
                    <OngoingEtlTaskDistribution task={data} etlStats={etlStats} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
