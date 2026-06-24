import React from "react";
import {
    RichPanel,
    RichPanelActions,
    RichPanelDetailItem,
    RichPanelDetails,
    RichPanelHeader,
    RichPanelInfo,
    RichPanelSelect,
} from "components/common/RichPanel";
import {
    ConnectionStringItem,
    DestinationUrlItem,
    EmptyScriptsWarning,
    ICanShowTransformationScriptPreview,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
} from "../../shared/shared";
import { useAppUrls } from "hooks/useAppUrls";
import { OngoingTaskRavenEtlInfo } from "components/models/tasks";
import { OngoingEtlTaskDistribution } from "../partials/OngoingEtlTaskDistribution";
import Collapse from "react-bootstrap/Collapse";
import Form from "react-bootstrap/Form";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import { EtlPanelBaseProps, useEtlPanel } from "./etlPanelUtils";
import { EtlPanelErrors, EtlPanelHealthBadge, EtlPanelProgressItem, EtlPanelToggleButton } from "./EtlPanelComponents";

type RavenEtlPanelProps = EtlPanelBaseProps<OngoingTaskRavenEtlInfo>;

export function RavenEtlPanel(props: RavenEtlPanelProps & ICanShowTransformationScriptPreview) {
    const { data, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState, etlStats } = props;

    const { forCurrentDatabase } = useAppUrls();
    const editUrl = forCurrentDatabase.editRavenEtl(data.shared.taskId)();

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
    const connectionStringDefined = !!data.shared.destinationDatabase;

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
                    <Icon icon="ravendb-etl" />
                    RavenDB ETL
                </RichPanelDetailItem>
                <ConnectionStringItem
                    connectionStringDefined={connectionStringDefined}
                    canEdit={canEdit}
                    connectionStringName={data.shared.connectionStringName}
                    connectionStringType="Raven"
                    databaseName={databaseName}
                />
                <RichPanelDetailItem label="Destination Database" title={data.shared.destinationDatabase}>
                    <div className="text-truncate" style={{ maxWidth: "200px" }}>
                        {data.shared.destinationDatabase}
                    </div>
                </RichPanelDetailItem>
                <DestinationUrlItem destinationUrl={data.shared.destinationUrl} />
                <RichPanelDetailItem
                    label="Topology Discovery URLs"
                    title={data.shared.topologyDiscoveryUrls.join(", ")}
                >
                    <div className="text-truncate" style={{ maxWidth: "200px" }}>
                        {data.shared.topologyDiscoveryUrls.join(", ")}
                    </div>
                </RichPanelDetailItem>
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
