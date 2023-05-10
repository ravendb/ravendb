import React, { useCallback, useState } from "react";
import { useAccessManager } from "hooks/useAccessManager";
import {
    Button,
    ButtonGroup,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Spinner,
    UncontrolledDropdown,
} from "reactstrap";
import { useAppDispatch } from "components/store";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useEventsCollector } from "hooks/useEventsCollector";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import { Icon } from "components/common/Icon";
import {
    changeDatabasesLockMode,
    confirmSetLockMode,
    confirmToggleDatabases,
    toggleDatabases,
} from "components/pages/resources/databases/store/databasesViewActions";
import { databaseActions } from "components/common/shell/databaseSliceActions";

interface DatabasesSelectActionsProps {
    selectedDatabases: DatabaseSharedInfo[];
    databaseNames: string[];
    setSelectedDatabaseNames: (x: string[]) => void;
}

export function DatabasesSelectActions({
    selectedDatabases,
    databaseNames,
    setSelectedDatabaseNames,
}: DatabasesSelectActionsProps) {
    const { isOperatorOrAbove } = useAccessManager();
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
        const result = await dispatch(databaseActions.confirmDeleteDatabases(selectedDatabases));

        if (result.can) {
            setDeleteChanges(true);
            try {
                await dispatch(databaseActions.deleteDatabases(result.databases, result.keepFiles));
            } finally {
                setDeleteChanges(false);
            }
        }
    };

    return (
        <div className="position-relative">
            <Checkbox
                selected={selectedDatabases.length > 0}
                indeterminate={0 < selectedDatabases.length && selectedDatabases.length < databaseNames.length}
                toggleSelection={toggleSelectAll}
                color="primary"
                title="Select all or none"
                size="lg"
                className="ms-5"
            >
                <span className="small-label">Select All</span>
            </Checkbox>

            <SelectionActions active={anythingSelected && !toggleChanges}>
                <div className="d-flex align-items-center">
                    <div className="lead me-4 text-nowrap">
                        <strong className="text-emphasis me-1">{selectedDatabases.length}</strong> selected
                    </div>
                    <ButtonGroup className="gap-2">
                        {isOperatorOrAbove() && (
                            <UncontrolledDropdown>
                                <DropdownToggle
                                    caret
                                    disabled={!anythingSelected || toggleChanges}
                                    title="Set the status (enabled/disabled) of selected databases"
                                    className="rounded-pill"
                                >
                                    {toggleChanges ? <Spinner size="sm" /> : <Icon icon="play" />} Set state
                                </DropdownToggle>
                                <DropdownMenu>
                                    <DropdownItem title="Enable" onClick={() => onToggleDatabases(true)}>
                                        <Icon icon="unlock" />
                                        <span>Enable</span>
                                    </DropdownItem>
                                    <DropdownItem title="Disable" onClick={() => onToggleDatabases(false)}>
                                        <Icon icon="lock" />
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
                                    className="rounded-pill"
                                >
                                    {lockChanges ? <Spinner size="sm" /> : <Icon icon="lock" />} Set delete lock mode
                                </DropdownToggle>
                                <DropdownMenu>
                                    <DropdownItem
                                        onClick={() => onChangeLockMode("Unlock")}
                                        title="Allow to delete selected databases"
                                    >
                                        <Icon icon="trash" addon="check" />
                                        <span>Allow databases delete</span>
                                    </DropdownItem>
                                    <DropdownItem
                                        onClick={() => onChangeLockMode("PreventDeletesIgnore")}
                                        title="Prevent deletion of selected databases. An error will not be thrown if an app attempts to delete."
                                    >
                                        <Icon icon="trash" addon="cancel" />
                                        <span>Prevent databases delete</span>
                                    </DropdownItem>
                                    <DropdownItem
                                        onClick={() => onChangeLockMode("PreventDeletesError")}
                                        title="Prevent deletion of selected databases. An error will be thrown if an app attempts to delete."
                                    >
                                        <Icon icon="trash" addon="exclamation" />
                                        <span>Prevent databases delete (Error)</span>
                                    </DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        )}
                        {isOperatorOrAbove() && (
                            <Button
                                color="danger"
                                onClick={onDelete}
                                disabled={!canDeleteSelection || deleteChanges}
                                className="rounded-pill"
                            >
                                {deleteChanges ? <Spinner size="sm" /> : <i className="icon-trash" />}
                                Delete
                            </Button>
                        )}

                        <Button onClick={toggleSelectAll} color="link" className="ms-2">
                            Cancel
                        </Button>
                    </ButtonGroup>
                </div>
            </SelectionActions>
        </div>
    );
}
