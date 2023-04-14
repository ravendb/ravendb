import React, { useCallback, useMemo, useState } from "react";
import { useAccessManager } from "hooks/useAccessManager";
import { Button, DropdownItem, DropdownMenu, DropdownToggle, Spinner, UncontrolledDropdown } from "reactstrap";
import { useAppDispatch } from "components/store";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useEventsCollector } from "hooks/useEventsCollector";
import { ButtonGroupWithLabel } from "components/common/ButtonGroupWithLabel";
import { CheckboxTriple } from "components/common/CheckboxTriple";
import {
    changeDatabasesLockMode,
    confirmDeleteDatabases,
    confirmSetLockMode,
    confirmToggleDatabases,
    deleteDatabases,
    openCreateDatabaseDialog,
    openCreateDatabaseFromRestoreDialog,
    toggleDatabases,
} from "components/common/shell/databaseSliceActions";
import { Icon } from "components/common/Icon";

interface DatabasesToolbarActionsProps {
    selectedDatabases: DatabaseSharedInfo[];
    databaseNames: string[];
    setSelectedDatabaseNames: (x: string[]) => void;
}

export function DatabasesToolbarActions({
    selectedDatabases,
    databaseNames,
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
            setSelectedDatabaseNames(databaseNames);
        }
    }, [selectedDatabases.length, setSelectedDatabaseNames, databaseNames]);

    const databasesSelectionState = useMemo<checkbox>(() => {
        const selectedCount = selectedDatabases.length;
        const dbsCount = databaseNames.length;
        if (dbsCount > 0 && dbsCount === selectedCount) {
            return "checked";
        }

        if (selectedCount > 0) {
            return "some_checked";
        }

        return "unchecked";
    }, [selectedDatabases.length, databaseNames]);

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
                        <Icon icon="database" addon="plus" className="me-1"></Icon>
                        <span>New database</span>
                    </Button>
                    <DropdownToggle color="primary" caret></DropdownToggle>
                    <DropdownMenu>
                        <DropdownItem onClick={() => dispatch(openCreateDatabaseFromRestoreDialog())}>
                            <Icon icon="restore-backup" className="me-1"></Icon> New database from backup (Restore)
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
                            {deleteChanges ? <Spinner size="sm" /> : <Icon icon="trash" className="me-1"></Icon>}
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
                                {toggleChanges ? <Spinner size="sm" /> : <Icon icon="play" className="me-1"></Icon>}
                                <span>Set state...</span>
                            </DropdownToggle>
                            <DropdownMenu>
                                <DropdownItem title="Enable" onClick={() => onToggleDatabases(true)}>
                                    <Icon icon="unlock" className="me-1"></Icon>
                                    <span>Enable</span>
                                </DropdownItem>
                                <DropdownItem title="Disable" onClick={() => onToggleDatabases(false)}>
                                    <Icon icon="lock" className="me-1"></Icon>
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
                                {lockChanges ? <Spinner size="sm" /> : <Icon icon="lock" className="me-1"></Icon>}

                                <span>Set delete lock mode...</span>
                            </DropdownToggle>
                            <DropdownMenu>
                                <DropdownItem
                                    onClick={() => onChangeLockMode("Unlock")}
                                    title="Allow to delete selected databases"
                                >
                                    <Icon icon="trash" addon="check" className="me-1"></Icon>
                                    <span>Allow databases delete</span>
                                </DropdownItem>
                                <DropdownItem
                                    onClick={() => onChangeLockMode("PreventDeletesIgnore")}
                                    title="Prevent deletion of selected databases. An error will not be thrown if an app attempts to delete."
                                >
                                    <Icon icon="trash" addon="cancel" className="me-1"></Icon>
                                    <span>Prevent databases delete</span>
                                </DropdownItem>
                                <DropdownItem
                                    onClick={() => onChangeLockMode("PreventDeletesError")}
                                    title="Prevent deletion of selected databases. An error will be thrown if an app attempts to delete."
                                >
                                    <Icon icon="trash" addon="exclamation" className="me-1"></Icon>
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
