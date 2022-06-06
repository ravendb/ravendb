import database from "models/resources/database";
import { OngoingTaskInfo, OngoingTaskSharedInfo } from "../../../../models/tasks";
import useBoolean from "hooks/useBoolean";
import React, { useCallback } from "react";
import router from "plugins/router";
import { withPreventDefault } from "../../../../utils/common";
import { RichPanelDetailItem } from "../../../../common/RichPanel";

export interface BaseOngoingTaskPanelProps<T extends OngoingTaskInfo> {
    db: database;
    data: T;
    onDelete: (task: OngoingTaskSharedInfo) => void;
    toggleState: (task: OngoingTaskSharedInfo, enable: boolean) => void;
}

export function useTasksOperations(editUrl: string, props: BaseOngoingTaskPanelProps<OngoingTaskInfo>) {
    const { onDelete, data, toggleState } = props;
    const { value: detailsVisible, toggle: toggleDetails } = useBoolean(false);

    const onEdit = useCallback(() => {
        router.navigate(editUrl);
    }, [editUrl]);

    const onDeleteHandler = useCallback(() => {
        onDelete(data.shared);
    }, [data.shared, onDelete]);

    const toggleStateHandler = useCallback((e: boolean) => toggleState(data.shared, e), [data]);

    return {
        detailsVisible,
        toggleDetails,
        onEdit,
        onDeleteHandler,
        toggleStateHandler,
    };
}

export function OngoingTaskName(props: { task: OngoingTaskInfo; canEdit: boolean; editUrl: string }) {
    const { task, canEdit, editUrl } = props;
    return (
        <div className="panel-name flex-grow">
            <h3 title={"Task name: " + task.shared.taskName}>
                {canEdit && (
                    <a href={editUrl}>
                        <span>{task.shared.taskName}</span>
                    </a>
                )}
                {!canEdit && <div>{task.shared.taskName}</div>}
            </h3>
        </div>
    );
}

export function OngoingTaskStatus(props: {
    task: OngoingTaskInfo;
    canEdit: boolean;
    toggleState: (enabled: boolean) => void;
}) {
    const { task, canEdit, toggleState } = props;
    return (
        <div className="btn-group">
            <button type="button" className="btn dropdown-toggle" data-toggle="dropdown" disabled={!canEdit}>
                <span>{task.shared.taskState}</span>
                <span className="caret"></span>
            </button>
            <ul className="dropdown-menu">
                <li>
                    <a href="#" onClick={withPreventDefault(() => toggleState(true))}>
                        <span>Enable</span>
                    </a>
                </li>
                <li>
                    <a href="#" onClick={withPreventDefault(() => toggleState(false))}>
                        <span>Disable</span>
                    </a>
                </li>
            </ul>
        </div>
    );
}

export function OngoingTaskActions(props: {
    canEdit: boolean;
    task: OngoingTaskInfo;
    toggleDetails: () => void;
    onEdit: () => void;
    onDelete: () => void;
}) {
    const { canEdit, task, onEdit, onDelete, toggleDetails } = props;

    return (
        <div className="actions-container">
            <div className="actions">
                <button className="btn btn-default" type="button" onClick={toggleDetails} title="Click for details">
                    <i className="icon-info"></i>
                </button>
                {!task.shared.serverWide && (
                    <button type="button" className="btn btn-default" title="Edit task" onClick={onEdit}>
                        <i className="icon-edit"></i>
                    </button>
                )}

                {!task.shared.serverWide && (
                    <button
                        className="btn btn-danger"
                        type="button"
                        disabled={!canEdit}
                        onClick={onDelete}
                        title="Delete task"
                    >
                        <i className="icon-trash"></i>
                    </button>
                )}
            </div>
        </div>
    );
}

export function ConnectionStringItem(props: {
    canEdit: boolean;
    connectionStringName: string;
    connectionStringsUrl: string;
    connectionStringDefined: boolean;
}) {
    const { canEdit, connectionStringDefined, connectionStringName, connectionStringsUrl } = props;

    if (connectionStringDefined) {
        return (
            <RichPanelDetailItem>
                Connection String:
                <div className="value">
                    {canEdit ? (
                        <a title="Connection string name" target="_blank" href={connectionStringsUrl}>
                            {connectionStringName}
                        </a>
                    ) : (
                        <div>{connectionStringName}</div>
                    )}
                </div>
            </RichPanelDetailItem>
        );
    }

    return (
        <RichPanelDetailItem>
            Connection String:
            <div className="value text-danger">
                <i className="icon-danger text-danger"></i>
                This connection string is not defined.
            </div>
        </RichPanelDetailItem>
    );
}
