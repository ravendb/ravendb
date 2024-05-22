import React, { useState } from "react";
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { Button, DropdownItem, DropdownMenu, DropdownToggle, Spinner, UncontrolledDropdown } from "reactstrap";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import genUtils = require("common/generalUtils");
import ResetIndexesButton from "components/pages/database/indexes/list/partials/ResetIndexesButton";
import { IndexSharedInfo } from "components/models/indexes";
import { ExportIndexes } from "components/pages/database/indexes/list/ExportIndexes";
import { todo } from "common/developmentHelper";

interface IndexSelectActionProps {
    indexNames: string[];
    selectedIndexes: string[];
    replacements: IndexSharedInfo[];
    deleteSelectedIndexes: () => Promise<void>;
    startSelectedIndexes: () => Promise<void>;
    disableSelectedIndexes: () => Promise<void>;
    pauseSelectedIndexes: () => Promise<void>;
    resetSelectedIndexes: (mode?: Raven.Client.Documents.Indexes.IndexResetMode) => void;
    setLockModeSelectedIndexes: (lockMode: IndexLockMode) => Promise<void>;
    toggleSelectAll: () => void;
    onCancel: () => void;
}

todo("Feature", "Damian", "Add logic for Export indexes");

export default function IndexSelectAction(props: IndexSelectActionProps) {
    const {
        indexNames,
        selectedIndexes,
        replacements,
        deleteSelectedIndexes,
        startSelectedIndexes,
        disableSelectedIndexes,
        pauseSelectedIndexes,
        resetSelectedIndexes,
        setLockModeSelectedIndexes,
        toggleSelectAll,
        onCancel,
    } = props;

    const [globalLockChanges] = useState(false);
    // TODO: IDK I just wanted it to compile

    const selectionState = genUtils.getSelectionState(indexNames, selectedIndexes);

    const isResetDropdownVisible = !replacements.some((x) => selectedIndexes.includes(x.name));

    const [isExportIndexModalOpen, setExportIndexModalOpen] = useState(false);
    const toggleExportIndexModal = () => {
        setExportIndexModalOpen(!isExportIndexModalOpen);
    };

    return (
        <div className="position-relative">
            <Checkbox
                toggleSelection={toggleSelectAll}
                selected={selectionState === "AllSelected"}
                indeterminate={selectionState === "SomeSelected"}
                title="Select all or none"
                color="primary"
                size="lg"
                className="ms-3"
            >
                <span className="small-label">Select all</span>
            </Checkbox>

            <SelectionActions active={selectedIndexes.length > 0}>
                <div className="d-flex flex-wrap align-items-center justify-content-center gap-2">
                    <div className="lead text-nowrap">
                        <strong className="text-emphasis me-1">{selectedIndexes.length}</strong> selected
                    </div>
                    <div className="hstack gap-2 flex-wrap justify-content-center">
                        <Button
                            color="primary"
                            disabled={selectedIndexes.length === 0}
                            onClick={toggleExportIndexModal}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="index-export" />
                            <span>Export {selectedIndexes.length > 1 ? "indexes" : "index"}</span>
                        </Button>

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
                                <DropdownItem onClick={startSelectedIndexes} title="Start indexing">
                                    <Icon icon="play" /> <span>Start indexing</span>
                                </DropdownItem>
                                <DropdownItem onClick={disableSelectedIndexes} title="Disable indexing">
                                    <Icon icon="stop" color="danger" /> <span>Disable indexing</span>
                                </DropdownItem>
                                <DropdownItem onClick={pauseSelectedIndexes} title="Pause indexing until restart">
                                    <Icon icon="pause" color="warning" /> <span>Pause indexing until restart</span>
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
                                <DropdownItem
                                    onClick={() => setLockModeSelectedIndexes("Unlock")}
                                    title="Unlock selected indexes"
                                >
                                    <Icon icon="unlock" /> <span>Unlock</span>
                                </DropdownItem>
                                <DropdownItem
                                    onClick={() => setLockModeSelectedIndexes("LockedIgnore")}
                                    title="Lock selected indexes"
                                >
                                    <Icon icon="lock" /> <span>Lock</span>
                                </DropdownItem>
                                <DropdownItem divider />
                                <DropdownItem
                                    onClick={() => setLockModeSelectedIndexes("LockedError")}
                                    title="Lock (Error) selected indexes"
                                >
                                    <Icon icon="lock-error" /> <span>Lock (Error)</span>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>

                        <ResetIndexesButton
                            resetIndex={resetSelectedIndexes}
                            isDropdownVisible={isResetDropdownVisible}
                            isRounded
                        />

                        <Button
                            color="danger"
                            disabled={selectedIndexes.length === 0}
                            onClick={deleteSelectedIndexes}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="trash" />
                            <span>Delete</span>
                        </Button>
                    </div>
                    <Button onClick={onCancel} color="link">
                        Cancel
                    </Button>
                    {isExportIndexModalOpen && <ExportIndexes toggle={toggleExportIndexModal} />}
                </div>
            </SelectionActions>
        </div>
    );
}
