import React, { useState } from "react";
import { useAccessManager } from "hooks/useAccessManager";
import { Button, DropdownItem, DropdownMenu, DropdownToggle, Spinner, UncontrolledDropdown } from "reactstrap";
import {
    changeDatabasesLockMode,
    openCreateDatabaseDialog,
    openCreateDatabaseFromRestoreDialog,
    openDeleteDatabasesDialog,
} from "components/common/shell/databasesSlice";
import { useAppDispatch } from "components/store";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useEventsCollector } from "hooks/useEventsCollector";

interface DatabasesToolbarActionsProps {
    selectedDatabases: DatabaseSharedInfo[];
}

export function DatabasesToolbarActions(props: DatabasesToolbarActionsProps) {
    const { selectedDatabases } = props;
    const { isOperatorOrAbove } = useAccessManager();
    const canCreateNewDatabase = isOperatorOrAbove();
    const { reportEvent } = useEventsCollector();

    const [lockChanges, setLockChanges] = useState(false);

    const dispatch = useAppDispatch();

    const canDeleteSelection = selectedDatabases.some((x) => x.lockMode === "Unlock");
    const anythingSelected = selectedDatabases.length > 0;

    //TODO: put delete button into named group

    const changeLockMode = async (lockMode: DatabaseLockMode) => {
        setLockChanges(true);
        try {
            reportEvent("databases", "set-lock-mode", lockMode);

            await dispatch(changeDatabasesLockMode(selectedDatabases, lockMode));
        } finally {
            setLockChanges(false);
        }
    };

    return (
        <div className="actions d-flex justify-content-end">
            <div className="mx-3 d-flex">
                {isOperatorOrAbove() && (
                    <Button
                        color="danger"
                        onClick={() => dispatch(openDeleteDatabasesDialog(selectedDatabases))}
                        disabled={!canDeleteSelection}
                    >
                        <i className="icon-trash" />
                        <span>Delete</span>
                    </Button>
                )}

                {/*
            <div className="btn-group-label"
                 data-bind="css: { active: selectedDatabases().length }, visible: accessManager.canSetState || accessManager.canDelete"
                 data-label="Selection" role="group">
                <div className="btn-group" data-bind="visible: accessManager.canSetState">
                    <button type="button" className="btn btn-default dropdown-toggle"
                            title="Set the status (enabled/disabled) of selected databases"
                            data-bind="enable: selectedDatabases().length && !spinners.globalToggleDisable(), css: { 'btn-spinner': spinners.globalToggleDisable() }"
                            data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
                        <i className="icon-play"></i><span>Set state...</span>
                        <span className="caret"></span>
                        <span className="sr-only">Toggle Dropdown</span>
                    </button>
                    <ul className="dropdown-menu">
                        <li data-bind="click: enableSelectedDatabases">
                            <a href="#" title="Enable">
                                <i className="icon-unlock"></i>
                                <span>Enable</span>
                            </a>
                        </li>
                        <li data-bind="click: disableSelectedDatabases">
                            <a href="#" title="Disable">
                                <i className="icon-lock"></i>
                                <span>Disable</span>
                            </a>
                        </li>
                    </ul>
                </div>
            </div>
            */}

                {isOperatorOrAbove() && (
                    <UncontrolledDropdown>
                        <DropdownToggle
                            title="Set the delete lock mode for the selected databases"
                            caret
                            disabled={!anythingSelected || lockChanges}
                        >
                            {lockChanges ? <Spinner size="sm" /> : <i className="icon-lock" />}

                            <span>Set delete lock mode...</span>
                        </DropdownToggle>
                        <DropdownMenu>
                            <DropdownItem
                                onClick={() => changeLockMode("Unlock")}
                                title="Allow to delete selected databases"
                            >
                                <i className="icon-trash-cutout icon-addon-check"></i>
                                <span>Allow databases delete</span>
                            </DropdownItem>
                            <DropdownItem
                                onClick={() => changeLockMode("PreventDeletesIgnore")}
                                title="Prevent deletion of selected databases. An error will not be thrown if an app attempts to delete."
                            >
                                <i className="icon-trash-cutout icon-addon-cancel"></i>
                                <span>Prevent databases delete</span>
                            </DropdownItem>
                            <DropdownItem
                                onClick={() => changeLockMode("PreventDeletesError")}
                                title="Prevent deletion of selected databases. An error will be thrown if an app attempts to delete."
                            >
                                <i className="icon-trash-cutout icon-addon-exclamation"></i>
                                <span>Prevent databases delete (Error)</span>
                            </DropdownItem>
                        </DropdownMenu>
                    </UncontrolledDropdown>
                )}
            </div>

            {canCreateNewDatabase && (
                <UncontrolledDropdown group>
                    <Button color="primary" onClick={() => dispatch(openCreateDatabaseDialog())}>
                        <i className="icon-new-database" />
                        <span>New database</span>
                    </Button>
                    <DropdownToggle color="primary" caret></DropdownToggle>
                    <DropdownMenu>
                        <DropdownItem onClick={() => dispatch(openCreateDatabaseFromRestoreDialog())}>
                            <i className="icon-restore-backup" /> New database from backup (Restore)
                        </DropdownItem>
                    </DropdownMenu>
                </UncontrolledDropdown>
            )}
        </div>
    );
}
