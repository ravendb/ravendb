import React, { useCallback } from "react";
import {
    BaseOngoingTaskPanelProps,
    ConnectionStringItem,
    EmptyScriptsWarning,
    ICanShowTransformationScriptPreview,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskResponsibleNode,
    OngoingTaskStatus,
    useTasksOperations,
} from "../../shared/shared";
import { OngoingTaskOlapEtlInfo } from "components/models/tasks";
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
import { OngoingEtlTaskDistribution } from "./OngoingEtlTaskDistribution";
import { Collapse, Input } from "reactstrap";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";

type OlapEtlPanelProps = BaseOngoingTaskPanelProps<OngoingTaskOlapEtlInfo>;

function Details(props: OlapEtlPanelProps & { canEdit: boolean }) {
    const { data, canEdit } = props;
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const connectionStringsUrl = appUrl.forConnectionStrings(databaseName, "Olap", data.shared.connectionStringName);

    return (
        <RichPanelDetails>
            {data.shared.destinations.map((dst) => (
                <RichPanelDetailItem label="Destination" key={dst}>
                    {dst}
                </RichPanelDetailItem>
            ))}
            <ConnectionStringItem
                connectionStringDefined
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
            <EmptyScriptsWarning task={data} />
        </RichPanelDetails>
    );
}

export function OlapEtlPanel(props: OlapEtlPanelProps & ICanShowTransformationScriptPreview) {
    const { data, showItemPreview, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } = props;

    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = hasDatabaseAdminAccess && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editOlapEtl(data.shared.taskId)();

    const { detailsVisible, toggleDetails, onEdit } = useTasksOperations(editUrl, props);

    const showPreview = useCallback(
        (transformationName: string) => {
            showItemPreview(data, transformationName);
        },
        [data, showItemPreview]
    );

    return (
        <RichPanel>
            <RichPanelHeader>
                <RichPanelInfo>
                    {canEdit && (
                        <RichPanelSelect>
                            <Input
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
                    />
                </RichPanelActions>
            </RichPanelHeader>
            <Collapse isOpen={detailsVisible}>
                <Details {...props} canEdit={canEdit} />
                <OngoingEtlTaskDistribution task={data} showPreview={showPreview} />
            </Collapse>
        </RichPanel>
    );
}
