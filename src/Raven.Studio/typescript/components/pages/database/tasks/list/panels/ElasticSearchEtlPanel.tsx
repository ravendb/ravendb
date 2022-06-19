import React, { useCallback } from "react";
import {
    BaseOngoingTaskPanelProps,
    ConnectionStringItem,
    ICanShowTransformationScriptPreview,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskStatus,
    useTasksOperations,
} from "../shared";
import { OngoingTaskElasticSearchEtlInfo } from "../../../../../models/tasks";
import { useAccessManager } from "hooks/useAccessManager";
import { useAppUrls } from "hooks/useAppUrls";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import { OngoingEtlTaskDistribution } from "./OngoingEtlTaskDistribution";

type ElasticSearchEtlPanelProps = BaseOngoingTaskPanelProps<OngoingTaskElasticSearchEtlInfo>;

function Details(props: ElasticSearchEtlPanelProps & { canEdit: boolean }) {
    const { data, canEdit, db } = props;
    const { appUrl } = useAppUrls();
    const connectionStringsUrl = appUrl.forConnectionStrings(db, "elasticSearch", data.shared.connectionStringName);

    return (
        <RichPanelDetails>
            <ConnectionStringItem
                connectionStringDefined
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
            {data.shared.nodesUrls.map((nodeUrl) => (
                <RichPanelDetailItem key={nodeUrl}>
                    Node URL:
                    <div className="value">{nodeUrl}</div>
                </RichPanelDetailItem>
            ))}
        </RichPanelDetails>
    );
}

export function ElasticSearchEtlPanel(props: ElasticSearchEtlPanelProps & ICanShowTransformationScriptPreview) {
    const { db, data, showItemPreview } = props;

    const { isAdminAccessOrAbove } = useAccessManager();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = isAdminAccessOrAbove(db) && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editElasticSearchEtl(data.shared.taskId)();

    const { detailsVisible, toggleDetails, toggleStateHandler, onEdit, onDeleteHandler } = useTasksOperations(
        editUrl,
        props
    );

    const showPreview = useCallback(
        (transformationName: string) => {
            showItemPreview(data, transformationName);
        },
        [data]
    );

    return (
        <RichPanel>
            <RichPanelHeader>
                <OngoingTaskName task={data} canEdit={canEdit} editUrl={editUrl} />
                <OngoingTaskStatus task={data} canEdit={canEdit} toggleState={toggleStateHandler} />
                <OngoingTaskActions
                    task={data}
                    canEdit={canEdit}
                    onEdit={onEdit}
                    onDelete={onDeleteHandler}
                    toggleDetails={toggleDetails}
                />
            </RichPanelHeader>
            {detailsVisible && <Details {...props} canEdit={canEdit} />}
            {detailsVisible && <OngoingEtlTaskDistribution task={data} showPreview={showPreview} />}
        </RichPanel>
    );
}
