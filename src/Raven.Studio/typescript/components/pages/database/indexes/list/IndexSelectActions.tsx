import React, { useState } from "react";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import ResetIndexesButton from "components/pages/database/indexes/list/partials/ResetIndexesButton";
import { IndexSharedInfo } from "components/models/indexes";
import { ExportIndexes } from "components/pages/database/indexes/list/migration/export/ExportIndexes";
import useBoolean from "components/hooks/useBoolean";
import Dropdown from "react-bootstrap/Dropdown";
import genUtils = require("common/generalUtils");
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;

interface IndexSelectActionProps {
    allIndexes: IndexSharedInfo[];
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

export default function IndexSelectAction(props: IndexSelectActionProps) {
    const {
        allIndexes,
        selectedIndexes,
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

    const { value: isExportIndexModalOpen, toggle: toggleIsExportIndexModalOpen } = useBoolean(false);

    const indexNames = allIndexes.map((x) => x.name);
    const selectionState = genUtils.getSelectionState(indexNames, selectedIndexes);

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
                            variant="primary"
                            disabled={selectedIndexes.length === 0}
                            onClick={toggleIsExportIndexModalOpen}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="index-export" />
                            <span>Export {selectedIndexes.length > 1 ? "indexes" : "index"}</span>
                        </Button>
                        {isExportIndexModalOpen && (
                            <ExportIndexes
                                toggle={toggleIsExportIndexModalOpen}
                                indexes={allIndexes}
                                selectedNames={selectedIndexes}
                            />
                        )}

                        <Dropdown>
                            <Dropdown.Toggle
                                variant="secondary"
                                title="Set the indexing state for the selected indexes"
                                disabled={selectedIndexes.length === 0}
                                data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges()"
                                className="rounded-pill"
                            >
                                {globalLockChanges && <Spinner size="sm" className="me-1" />}
                                {!globalLockChanges && <Icon icon="play" />}
                                Set indexing state
                            </Dropdown.Toggle>
                            <Dropdown.Menu>
                                <Dropdown.Item onClick={startSelectedIndexes} title="Start indexing">
                                    <Icon icon="play" /> <span>Start indexing</span>
                                </Dropdown.Item>
                                <Dropdown.Item onClick={disableSelectedIndexes} title="Disable indexing">
                                    <Icon icon="stop" color="danger" /> <span>Disable indexing</span>
                                </Dropdown.Item>
                                <Dropdown.Item onClick={pauseSelectedIndexes} title="Pause indexing until restart">
                                    <Icon icon="pause" color="warning" /> <span>Pause indexing until restart</span>
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>

                        <Dropdown>
                            <Dropdown.Toggle
                                variant="secondary"
                                title="Set the lock mode for the selected indexes"
                                disabled={selectedIndexes.length === 0}
                                data-bind="enable: $root.globalIndexingStatus() === 'Running' && selectedIndexesName().length && !spinners.globalLockChanges()"
                                className="rounded-pill"
                            >
                                {globalLockChanges && <Spinner size="sm" className="me-1" />}
                                {!globalLockChanges && <Icon icon="lock" />}
                                Set lock mode
                            </Dropdown.Toggle>

                            <Dropdown.Menu>
                                <Dropdown.Item
                                    onClick={() => setLockModeSelectedIndexes("Unlock")}
                                    title="Unlock selected indexes"
                                >
                                    <Icon icon="unlock" /> <span>Unlock</span>
                                </Dropdown.Item>
                                <Dropdown.Item
                                    onClick={() => setLockModeSelectedIndexes("LockedIgnore")}
                                    title="Lock selected indexes"
                                >
                                    <Icon icon="lock" /> <span>Lock</span>
                                </Dropdown.Item>
                                <Dropdown.Divider />
                                <Dropdown.Item
                                    onClick={() => setLockModeSelectedIndexes("LockedError")}
                                    title="Lock (Error) selected indexes"
                                >
                                    <Icon icon="lock-error" /> <span>Lock (Error)</span>
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>
                        <ResetIndexesButton isRounded resetIndex={resetSelectedIndexes} />
                        <Button
                            variant="danger"
                            disabled={selectedIndexes.length === 0}
                            onClick={deleteSelectedIndexes}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="trash" />
                            <span>Delete</span>
                        </Button>
                    </div>
                    <Button onClick={onCancel} variant="link">
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
