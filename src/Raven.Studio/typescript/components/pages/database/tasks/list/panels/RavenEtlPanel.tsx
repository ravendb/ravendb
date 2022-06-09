import React, { useCallback } from "react";
import { useAccessManager } from "hooks/useAccessManager";
import { RichPanel, RichPanelDetailItem, RichPanelDetails, RichPanelHeader } from "../../../../../common/RichPanel";
import {
    ConnectionStringItem,
    ICanShowTransformationScriptPreview,
    OngoingTaskActions,
    OngoingTaskName,
    OngoingTaskStatus,
} from "../shared";
import { useAppUrls } from "hooks/useAppUrls";
import { OngoingTaskInfo, OngoingTaskRavenEtlInfo } from "../../../../../models/tasks";
import { BaseOngoingTaskPanelProps, useTasksOperations } from "../shared";
import { OngoingTaskDistribution } from "./OngoingTaskDistribution";

type RavenEtlPanelProps = BaseOngoingTaskPanelProps<OngoingTaskRavenEtlInfo>;

function findScriptsWithOutMatchingDocuments(data: OngoingTaskInfo): string[] {
    const perScriptCounts = new Map<string, number>();
    data.nodesInfo.forEach((node) => {
        if (node.progress) {
            node.progress.forEach((progress) => {
                const transformationName = progress.transformationName;
                perScriptCounts.set(
                    transformationName,
                    (perScriptCounts.get(transformationName) ?? 0) + progress.global.total
                );
            });
        }
    });

    return Array.from(perScriptCounts.entries())
        .filter((x) => x[1] === 0)
        .map((x) => x[0]);
}

function Details(props: RavenEtlPanelProps & { canEdit: boolean }) {
    const { data, canEdit, db } = props;
    const connectionStringDefined = !!data.shared.destinationDatabase;
    const { appUrl } = useAppUrls();
    const connectionStringsUrl = appUrl.forConnectionStrings(db, "ravendb", data.shared.connectionStringName);

    const emptyScripts = findScriptsWithOutMatchingDocuments(data);

    return (
        <RichPanelDetails>
            <ConnectionStringItem
                connectionStringDefined={!!data.shared.destinationDatabase}
                canEdit={canEdit}
                connectionStringName={data.shared.connectionStringName}
                connectionStringsUrl={connectionStringsUrl}
            />
            {connectionStringDefined && (
                <RichPanelDetailItem>
                    Destination Database:
                    <div className="value">{data.shared.destinationDatabase}</div>
                </RichPanelDetailItem>
            )}
            <RichPanelDetailItem>
                Actual Destination URL:
                <div className="value">
                    {data.shared.destinationUrl ? (
                        <a href={data.shared.destinationUrl} target="_blank">
                            {data.shared.destinationUrl}
                        </a>
                    ) : (
                        <div>N/A</div>
                    )}
                </div>
            </RichPanelDetailItem>
            {data.shared.topologyDiscoveryUrls?.length > 0 && (
                <RichPanelDetailItem>
                    Topology Discovery URLs:
                    <div className="value">{data.shared.topologyDiscoveryUrls.join(", ")}</div>
                </RichPanelDetailItem>
            )}
            {emptyScripts.length > 0 && (
                <RichPanelDetailItem className="text-warning">
                    <small>
                        <i className="icon-warning" />
                        Following scripts don't match any documents: {emptyScripts.join(", ")}
                    </small>
                </RichPanelDetailItem>
            )}
        </RichPanelDetails>
    );
}

export function RavenEtlPanel(props: RavenEtlPanelProps & ICanShowTransformationScriptPreview) {
    const { db, data, showItemPreview } = props;

    const { isAdminAccessOrAbove } = useAccessManager();
    const { forCurrentDatabase } = useAppUrls();

    const canEdit = isAdminAccessOrAbove(db) && !data.shared.serverWide;
    const editUrl = forCurrentDatabase.editRavenEtl(data.shared.taskId)();

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
            {detailsVisible && <OngoingTaskDistribution task={data} showPreview={showPreview} />}
        </RichPanel>
    );
}
