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
} from "../../shared";
import { OngoingTaskSqlEtlInfo } from "components/models/tasks";
import { useAccessManager } from "hooks/useAccessManager";
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

type SqlEtlPanelProps = BaseOngoingTaskPanelProps<OngoingTaskSqlEtlInfo>;

function Details(props: SqlEtlPanelProps & { canEdit: boolean }) {
    const { data, canEdit, db } = props;
    const { appUrl } = useAppUrls();
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const connectionStringsUrl = appUrl.forConnectionStrings(db, "Sql", data.shared.connectionStringName);

    return (
        <RichPanelDetails>
            {connectionStringDefined && (
                <RichPanelDetailItem label="Destination" title="Destination <database>@<server>">
                    {(data.shared.destinationDatabase ?? "") + "@" + (data.shared.destinationServer ?? "")}
                </RichPanelDetailItem>
            )}
            <ConnectionStringItem
                connectionStringDefined={!!data.shared.destinationDatabase}
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
            <EmptyScriptsWarning task={data} />
        </RichPanelDetails>
    );
}

export function SqlEtlPanel(props: SqlEtlPanelProps & ICanShowTransformationScriptPreview) {
    const { db, data, showItemPreview, toggleSelection, isSelected, onTaskOperation, isDeleting, isTogglingState } =
        props;

    const { isAdminAccessOrAbove } = useAccessManager();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = isAdminAccessOrAbove(db) && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editSqlEtl(data.shared.taskId)();

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
                                checked={isSelected(data.shared.taskName)}
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
                        isTogglingState={isTogglingState(data.shared.taskName)}
                    />
                    <OngoingTaskActions
                        task={data}
                        canEdit={canEdit}
                        onEdit={onEdit}
                        onTaskOperation={onTaskOperation}
                        toggleDetails={toggleDetails}
                        isDeleting={isDeleting(data.shared.taskName)}
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
