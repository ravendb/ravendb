import React, { useCallback, useState } from "react";
import {
    Button,
    ButtonGroup,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Spinner,
    UncontrolledDropdown,
} from "reactstrap";
import { useAppDispatch, useAppSelector } from "components/store";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import { useEventsCollector } from "hooks/useEventsCollector";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import { Icon } from "components/common/Icon";
import {
    changeDatabasesLockMode,
    confirmToggleDatabases,
    toggleDatabases,
} from "components/pages/resources/databases/store/databasesViewActions";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import genUtils = require("common/generalUtils");
import useConfirm from "components/common/ConfirmDialog";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

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
    const isOperatorOrAbove = useAppSelector(accessManagerSelectors.isOperatorOrAbove);
    const { reportEvent } = useEventsCollector();

    const [lockChanges, setLockChanges] = useState(false);
    const [toggleChanges, setToggleChanges] = useState(false);
    const [deleteChanges, setDeleteChanges] = useState(false);

    const dispatch = useAppDispatch();
    const confirm = useConfirm();

    const canDeleteSelection = selectedDatabases.some((x) => x.lockMode === "Unlock");
    const anythingSelected = selectedDatabases.length > 0;
    const selectedDatabaseNames = selectedDatabases.map((x) => x.name);

    const selectionState = genUtils.getSelectionState(
        databaseNames,
        selectedDatabases.map((x) => x.name)
    );

    const toggleSelectAll = useCallback(() => {
        if (selectionState === "Empty") {
            setSelectedDatabaseNames([...selectedDatabaseNames, ...databaseNames]);
        } else {
            setSelectedDatabaseNames(selectedDatabaseNames.filter((x) => !databaseNames.includes(x)));
        }
    }, [databaseNames, selectedDatabaseNames, selectionState, setSelectedDatabaseNames]);

    if (!isOperatorOrAbove) {
        // no access
        return null;
    }

    const onChangeLockMode = async (lockMode: DatabaseLockMode) => {
        const dbs = selectedDatabases;

        reportEvent("databases", "set-lock-mode", lockMode);

        const isConfirmed = await confirm({
            title: "Do you want to change lock mode?`",
        });

        if (isConfirmed) {
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
        <div className="position-relative mt-3">
            <Checkbox
                selected={selectionState === "AllSelected"}
                indeterminate={selectionState === "SomeSelected"}
                toggleSelection={toggleSelectAll}
                color="primary"
                title="Select all or none"
                size="lg"
                className="ms-5"
            >
                <span className="small-label">Select All</span>
            </Checkbox>

            <SelectionActions active={anythingSelected && !toggleChanges}>
                <div className="d-flex align-items-center justify-content-center flex-wrap gap-2">
                    <div className="lead text-nowrap">
                        <strong className="text-emphasis me-1">{selectedDatabases.length}</strong> selected
                    </div>
                    <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                        {isOperatorOrAbove && (
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

                        {isOperatorOrAbove && (
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
                        {isOperatorOrAbove && (
                            <ButtonWithSpinner
                                color="danger"
                                onClick={onDelete}
                                disabled={!canDeleteSelection || deleteChanges}
                                className="rounded-pill flex-grow-0"
                                icon="trash"
                                isSpinning={deleteChanges}
                            >
                                Delete
                            </ButtonWithSpinner>
                        )}
                    </ButtonGroup>
                    <Button onClick={() => setSelectedDatabaseNames([])} color="link">
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
