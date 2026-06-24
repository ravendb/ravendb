import React from "react";
import {
    ConnectionStringItem,
    EmptyScriptsWarning,
    ICanShowTransformationScriptPreview,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
} from "../../shared/shared";
import { OngoingTaskAmazonSqsEtlInfo } from "components/models/tasks";
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
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { Icon } from "components/common/Icon";
import { EtlPanelBaseProps, useEtlPanel } from "./etlPanelUtils";
import { EtlPanelErrors, EtlPanelHealthBadge, EtlPanelProgressItem, EtlPanelToggleButton } from "./EtlPanelComponents";

type AmazonSqsEtlPanelProps = EtlPanelBaseProps<OngoingTaskAmazonSqsEtlInfo>;

export function AmazonSqsEtlPanel(props: AmazonSqsEtlPanelProps & ICanShowTransformationScriptPreview) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState, etlStats } = props;

    const { forCurrentDatabase } = useAppUrls();
    const editUrl = forCurrentDatabase.editAmazonSqsEtl(data.shared.taskId)();

    const {
        canEdit,
        goToTaskErrors,
        detailsVisible,
        toggleDetails,
        onEdit,
        showPreview,
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
                        toggleDetails={toggleDetails}
                        isDeleting={isDeleting(data.shared.taskId)}
                        isDetailsOpen={detailsVisible}
                        isEtl
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <RichPanelDetails>
                <EtlPanelToggleButton detailsVisible={detailsVisible} toggleDetails={toggleDetails} />
                <RichPanelDetailItem label="Type">
                    <Icon icon="amazon-sqs-etl" />
                    Amazon SQS ETL
                </RichPanelDetailItem>
                <ConnectionStringItem
                    connectionStringDefined
                    canEdit={canEdit}
                    connectionStringName={data.shared.connectionStringName}
                    connectionStringType="AmazonSqs"
                    databaseName={databaseName}
                />
                <EtlPanelHealthBadge taskHealth={taskHealth} />
                <EtlPanelErrors
                    errorCount={errorCount}
                    errorsByLocation={errorsByLocation}
                    goToTaskErrors={goToTaskErrors}
                />
                <EtlPanelProgressItem etlProgress={etlProgress} />
                <EmptyScriptsWarning task={data} />
            </RichPanelDetails>
            <Collapse in={detailsVisible}>
                <div>
                    <OngoingEtlTaskDistribution task={data} showPreview={showPreview} etlStats={etlStats} />
                </div>
            </Collapse>
        </RichPanel>
    );
}
