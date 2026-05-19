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
import { OngoingTaskSqlEtlInfo } from "components/models/tasks";
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
import { EtlPanelErrors, EtlPanelHealthBadge, EtlPanelProgressItem, EtlPanelToggleButton } from "./EtlPanelComponents";

type SqlEtlPanelProps = EtlPanelBaseProps<OngoingTaskSqlEtlInfo>;

export function SqlEtlPanel(props: SqlEtlPanelProps & ICanShowTransformationScriptPreview) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState, etlStats } = props;

    const { forCurrentDatabase, appUrl } = useAppUrls();
    const editUrl = forCurrentDatabase.editSqlEtl(data.shared.taskId)();

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
    const connectionStringsUrl = appUrl.forConnectionStrings(databaseName, "Sql", data.shared.connectionStringName);
    const connectionStringDefined = data.shared.connectionStringDefined;

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
                    <Icon icon="sql-etl" />
                    SQL ETL
                </RichPanelDetailItem>
                <ConnectionStringItem
                    connectionStringDefined={connectionStringDefined}
                    canEdit={canEdit}
                    connectionStringName={data.shared.connectionStringName}
                    connectionStringsUrl={connectionStringsUrl}
                />
                {connectionStringDefined && (
                    <RichPanelDetailItem label="Destination" title="Destination <database>@<server>">
                        {(data.shared.destinationDatabase ?? "") + "@" + (data.shared.destinationServer ?? "")}
                    </RichPanelDetailItem>
                )}
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
