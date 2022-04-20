import React, { useCallback } from "react";
import { useAccessManager } from "../../../hooks/useAccessManager";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";

export function DatabasesToolbarActions() {
    const { isOperatorOrAbove } = useAccessManager();
    const canCreateNewDatabase = isOperatorOrAbove();

    const newDatabase = useCallback(() => {
        const createDbView = new createDatabase("newDatabase");
        app.showBootstrapDialog(createDbView);
    }, []);

    return (
        <div className="databasesToolbar-actions">
            {/*
            <div className="btn-group-label"
                 data-bind="css: { active: selectedDatabases().length }, visible: accessManager.canSetState || accessManager.canDelete"
                 data-label="Selection" role="group">
                <button className="btn btn-danger"
                        data-bind="visible: accessManager.canDelete, enable: selectedDatabasesWithoutLock().length, click: deleteSelectedDatabases">
                    <i className="icon-trash" />
                    <span>Delete</span>
                </button>
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
                <div className="btn-group" data-bind="visible: accessManager.canDelete">
                    <button type="button" className="btn btn-default dropdown-toggle"
                            title="Set the delete lock mode for the selected databases"
                            data-bind="enable: selectedDatabases().length && !spinners.globalToggleDisable(), 
                                                       css: { 'btn-spinner': spinners.globalToggleDisable() }"
                            data-toggle="dropdown" aria-haspopup="true">
                        <i className="icon-lock"></i><span>Set delete lock mode...</span>
                        <span className="caret"></span>
                        <span className="sr-only">Toggle Dropdown</span>
                    </button>
                    <ul className="dropdown-menu">
                        <li data-bind="click: unlockSelectedDatabases">
                            <a href="#" title="Allow to delete selected databases">
                                <i className="icon-trash-cutout icon-addon-check"></i>
                                <span>Allow databases delete</span>
                            </a>
                        </li>
                        <li data-bind="click: lockSelectedDatabases">
                            <a href="#"
                               title="Prevent deletion of selected databases. An error will not be thrown if an app attempts to delete.">
                                <i className="icon-trash-cutout icon-addon-cancel"></i>
                                <span>Prevent databases delete</span>
                            </a>
                        </li>
                        <li data-bind="click: lockErrorSelectedDatabases">
                            <a href="#"
                               title="Prevent deletion of selected databases. An error will be thrown if an app attempts to delete.">
                                <i className="icon-trash-cutout icon-addon-exclamation"></i>
                                <span>Prevent databases delete (Error)</span>
                            </a>
                        </li>
                    </ul>
                </div>
            </div>
            */}
            {canCreateNewDatabase && (
                <div className="btn-group">
                    <button type="button" className="btn btn-primary" onClick={newDatabase}>
                        <i className="icon-new-database" />
                        <span>New database</span>
                    </button>
                    {/* TODO
                <button type="button" className="btn btn-primary dropdown-toggle" data-toggle="dropdown"
                        aria-haspopup="true" aria-expanded="false">
                    <span className="caret" />
                    <span className="sr-only">Toggle Dropdown</span>
                </button>
                <ul className="dropdown-menu dropdown-menu-right">
                    <li>
                        <a href="#" data-bind="click: newDatabaseFromBackup">
                            <i className="icon-restore-backup" /> New database from backup (Restore)
                        </a>
                    </li>
                    <li>
                        <a href="#" data-bind="click: newDatabaseFromLegacyDatafiles">
                            <i className="icon-restore-backup" /> New database from v3.x (legacy) data files
                        </a>
                    </li>
                </ul>
                */}
                </div>
            )}
        </div>
    );
}
