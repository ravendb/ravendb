import React, { useCallback, useState } from "react";

import { withPreventDefault } from "components/utils/common";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import {
    Button,
    ButtonGroup,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Spinner,
    UncontrolledDropdown,
} from "reactstrap";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";

interface IndexSelectActionProps {
    indexesCount: number;
    selectedIndexes: string[];
    deleteSelectedIndexes: () => Promise<void>;
    enableSelectedIndexes: () => Promise<void>;
    disableSelectedIndexes: () => Promise<void>;
    pauseSelectedIndexes: () => Promise<void>;
    resumeSelectedIndexes: () => Promise<void>;
    setLockModeSelectedIndexes: (lockMode: IndexLockMode) => Promise<void>;
    toggleSelectAll: () => void;
}

export default function IndexSelectAction(props: IndexSelectActionProps) {
    const {
        indexesCount,
        selectedIndexes,
        deleteSelectedIndexes,
        enableSelectedIndexes,
        disableSelectedIndexes,
        pauseSelectedIndexes,
        resumeSelectedIndexes,
        setLockModeSelectedIndexes,
        toggleSelectAll,
    } = props;

    const unlockSelectedIndexes = useCallback(
        async (e: React.MouseEvent<HTMLElement>) => {
            e.preventDefault();
            await setLockModeSelectedIndexes("Unlock");
        },
        [setLockModeSelectedIndexes]
    );

    const lockSelectedIndexes = useCallback(
        async (e: React.MouseEvent<HTMLElement>) => {
            e.preventDefault();
            await setLockModeSelectedIndexes("LockedIgnore");
        },
        [setLockModeSelectedIndexes]
    );

    const lockErrorSelectedIndexes = useCallback(
        async (e: React.MouseEvent<HTMLElement>) => {
            e.preventDefault();
            await setLockModeSelectedIndexes("LockedError");
        },
        [setLockModeSelectedIndexes]
    );

    const [globalLockChanges] = useState(false);
    // TODO: IDK I just wanted it to compile

    return (
        <div className="position-relative">
            <Checkbox
                toggleSelection={toggleSelectAll}
                selected={selectedIndexes.length > 0}
                indeterminate={selectedIndexes.length > 0 && selectedIndexes.length < indexesCount}
                title="Select all or none"
                color="primary"
                size="lg"
                className="ms-3"
            />

            <SelectionActions active={selectedIndexes.length > 0}>
                <div className="d-flex align-items-center">
                    <div className="lead me-4 text-nowrap">
                        <strong className="text-emphasis me-1">{selectedIndexes.length}</strong> selected
                    </div>
                    <ButtonGroup className="gap-2">
                        <UncontrolledDropdown>
                            <DropdownToggle
                                title="Set the indexing state for the selected indexes"
                                disabled={selectedIndexes.length === 0}
                                data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges()"
                                className="rounded-pill"
                                caret
                            >
                                {globalLockChanges && <Spinner size="sm" className="me-1" />}
                                {!globalLockChanges && <Icon icon="play" />}
                                Set indexing state
                            </DropdownToggle>

                            <DropdownMenu>
                                <DropdownItem
                                    onClick={withPreventDefault(enableSelectedIndexes)}
                                    title="Enable indexing"
                                >
                                    <Icon icon="play" /> <span>Enable</span>
                                </DropdownItem>
                                <DropdownItem
                                    onClick={withPreventDefault(disableSelectedIndexes)}
                                    title="Disable indexing"
                                >
                                    <Icon icon="stop" color="danger" /> <span>Disable</span>
                                </DropdownItem>
                                <DropdownItem divider />
                                <DropdownItem
                                    onClick={withPreventDefault(resumeSelectedIndexes)}
                                    title="Resume indexing"
                                >
                                    <Icon icon="play" /> <span>Resume</span>
                                </DropdownItem>
                                <DropdownItem onClick={withPreventDefault(pauseSelectedIndexes)} title="Pause indexing">
                                    <Icon icon="pause" color="warning" /> <span>Pause</span>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>

                        <UncontrolledDropdown>
                            <DropdownToggle
                                title="Set the lock mode for the selected indexes"
                                disabled={selectedIndexes.length === 0}
                                data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges()"
                                className="rounded-pill"
                                caret
                            >
                                {globalLockChanges && <Spinner size="sm" className="me-1" />}
                                {!globalLockChanges && <Icon icon="lock" />}
                                Set lock mode
                            </DropdownToggle>

                            <DropdownMenu>
                                <DropdownItem onClick={unlockSelectedIndexes} title="Unlock selected indexes">
                                    <Icon icon="unlock" /> <span>Unlock</span>
                                </DropdownItem>
                                <DropdownItem onClick={lockSelectedIndexes} title="Lock selected indexes">
                                    <Icon icon="lock" /> <span>Lock</span>
                                </DropdownItem>
                                <DropdownItem divider />
                                <DropdownItem onClick={lockErrorSelectedIndexes} title="Lock (Error) selected indexes">
                                    <Icon icon="lock-error" /> <span>Lock (Error)</span>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>
                        <Button
                            color="danger"
                            disabled={selectedIndexes.length === 0}
                            onClick={deleteSelectedIndexes}
                            className="rounded-pill"
                        >
                            <Icon icon="trash" />
                            <span>Delete</span>
                        </Button>
                    </ButtonGroup>
                    <Button onClick={toggleSelectAll} color="link" className="ms-2">
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
