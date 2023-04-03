import React, { useCallback, useMemo, useState } from "react";
import { useAccessManager } from "hooks/useAccessManager";
import { Button, DropdownItem, DropdownMenu, DropdownToggle, Spinner, UncontrolledDropdown } from "reactstrap";
import {
    changeDatabasesLockMode,
    confirmDeleteDatabases,
    confirmSetLockMode,
    confirmToggleDatabases,
    deleteDatabases,
    openCreateDatabaseDialog,
    openCreateDatabaseFromRestoreDialog,
    toggleDatabases,
} from "components/common/shell/databasesSlice";
import { useAppDispatch } from "components/store";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useEventsCollector } from "hooks/useEventsCollector";
import { ButtonGroupWithLabel } from "components/common/ButtonGroupWithLabel";
import { CheckboxTriple } from "components/common/CheckboxTriple";

interface DatabasesToolbarActionsProps {
    selectedDatabases: DatabaseSharedInfo[];
    filteredDatabases: DatabaseSharedInfo[];
    setSelectedDatabaseNames: (x: string[]) => void;
}

export function DatabasesToolbarActions({
    selectedDatabases,
    filteredDatabases,
    setSelectedDatabaseNames,
}: DatabasesToolbarActionsProps) {
    const { isOperatorOrAbove } = useAccessManager();
    const canCreateNewDatabase = isOperatorOrAbove();
    const { reportEvent } = useEventsCollector();

    const [lockChanges, setLockChanges] = useState(false);
    const [toggleChanges, setToggleChanges] = useState(false);
    const [deleteChanges, setDeleteChanges] = useState(false);

    const dispatch = useAppDispatch();

    const canDeleteSelection = selectedDatabases.some((x) => x.lockMode === "Unlock");
    const anythingSelected = selectedDatabases.length > 0;

    const toggleSelectAll = useCallback(() => {
        const selectedCount = selectedDatabases.length;

        if (selectedCount > 0) {
            setSelectedDatabaseNames([]);
        } else {
            setSelectedDatabaseNames(filteredDatabases.map((x) => x.name));
        }
    }, [selectedDatabases.length, setSelectedDatabaseNames, filteredDatabases]);

    const databasesSelectionState = useMemo<checkbox>(() => {
        const selectedCount = selectedDatabases.length;
        const dbsCount = filteredDatabases.length;
        if (dbsCount > 0 && dbsCount === selectedCount) {
            return "checked";
        }

        if (selectedCount > 0) {
            return "some_checked";
        }

        return "unchecked";
    }, [selectedDatabases.length, filteredDatabases]);

    if (!isOperatorOrAbove()) {
        // no access
        return null;
    }

    const onChangeLockMode = async (lockMode: DatabaseLockMode) => {
        const dbs = selectedDatabases;

        reportEvent("databases", "set-lock-mode", lockMode);

        const can = await dispatch(confirmSetLockMode());

        if (can) {
            setLockChanges(true);
            try {
                await dispatch(changeDatabasesLockMode(dbs, lockMode));
            } finally {
                setLockChanges(false);
            }
        }
    };

    const onToggleDatabases = async (enable: boolean) => {
        const dbs = selectedDatabases;
        const result = await dispatch(confirmToggleDatabases(dbs, enable));

        if (result) {
            setToggleChanges(true);
            try {
                await dispatch(toggleDatabases(dbs, enable));
            } finally {
                setToggleChanges(false);
            }
        }
    };

    const onDelete = async () => {
        const result = await dispatch(confirmDeleteDatabases(selectedDatabases));

        if (result.can) {
            setDeleteChanges(true);
            try {
                await dispatch(deleteDatabases(result.databases, result.keepFiles));
            } finally {
                setDeleteChanges(false);
            }
        }
    };

    return (
        <div className="actions d-flex justify-content-between">
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

            <div className="flex-horizontal">
                <CheckboxTriple
                    onChanged={toggleSelectAll}
                    state={databasesSelectionState}
                    title="Select all or none"
                />

                <ButtonGroupWithLabel label="Selection" className="margin-left-sm gap-1">
                    {isOperatorOrAbove() && (
                        <Button color="danger" onClick={onDelete} disabled={!canDeleteSelection || deleteChanges}>
                            {deleteChanges ? <Spinner size="sm" /> : <i className="icon-trash" />}
                            <span>Delete</span>
                        </Button>
                    )}

                    {isOperatorOrAbove() && (
                        <UncontrolledDropdown>
                            <DropdownToggle
                                caret
                                disabled={!anythingSelected || toggleChanges}
                                title="Set the status (enabled/disabled) of selected databases"
                            >
                                {toggleChanges ? <Spinner size="sm" /> : <i className="icon-play" />}
                                <span>Set state...</span>
                            </DropdownToggle>
                            <DropdownMenu>
                                <DropdownItem title="Enable" onClick={() => onToggleDatabases(true)}>
                                    <i className="icon-unlock"></i>
                                    <span>Enable</span>
                                </DropdownItem>
                                <DropdownItem title="Disable" onClick={() => onToggleDatabases(false)}>
                                    <i className="icon-lock"></i>
                                    <span>Disable</span>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>
                    )}

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
                                    onClick={() => onChangeLockMode("Unlock")}
                                    title="Allow to delete selected databases"
                                >
                                    <i className="icon-trash-cutout icon-addon-check"></i>
                                    <span>Allow databases delete</span>
                                </DropdownItem>
                                <DropdownItem
                                    onClick={() => onChangeLockMode("PreventDeletesIgnore")}
                                    title="Prevent deletion of selected databases. An error will not be thrown if an app attempts to delete."
                                >
                                    <i className="icon-trash-cutout icon-addon-cancel"></i>
                                    <span>Prevent databases delete</span>
                                </DropdownItem>
                                <DropdownItem
                                    onClick={() => onChangeLockMode("PreventDeletesError")}
                                    title="Prevent deletion of selected databases. An error will be thrown if an app attempts to delete."
                                >
                                    <i className="icon-trash-cutout icon-addon-exclamation"></i>
                                    <span>Prevent databases delete (Error)</span>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>
                    )}
                </ButtonGroupWithLabel>
            </div>
        </div>
    );
}
