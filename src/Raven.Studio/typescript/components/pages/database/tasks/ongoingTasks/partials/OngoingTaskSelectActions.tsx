import React from "react";
import genUtils from "common/generalUtils";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import Spinner from "react-bootstrap/Spinner";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import { OngoingTaskOperationConfirmType } from "../../shared/OngoingTaskOperationConfirm";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";

interface OngoingTaskSelectActionsProps {
    allTasks: number[];
    selectedTasks: number[];
    setSelectedTasks: (ids: number[]) => void;
    onTaskOperation: (x: OngoingTaskOperationConfirmType) => void;
    isTogglingState: boolean;
    isDeleting: boolean;
}

export default function OngoingTaskSelectActions(props: OngoingTaskSelectActionsProps) {
    const { allTasks, selectedTasks, setSelectedTasks, onTaskOperation, isTogglingState, isDeleting } = props;

    if (allTasks.length === 0) {
        return null;
    }

    const anythingSelected = selectedTasks.length > 0;
    const selectionState = genUtils.getSelectionState(allTasks, selectedTasks);

    const toggleSelectAll = () => {
        if (selectionState === "Empty") {
            setSelectedTasks([...selectedTasks, ...allTasks]);
        } else {
            setSelectedTasks(selectedTasks.filter((x) => !allTasks.includes(x)));
        }
    };

    return (
        <div className="position-relative mt-3 ms-3">
            <Checkbox
                selected={selectionState === "AllSelected"}
                indeterminate={selectionState === "SomeSelected"}
                toggleSelection={toggleSelectAll}
                color="primary"
                title="Select all or none"
                size="lg"
            >
                <span className="small-label">Select All</span>
            </Checkbox>

            <SelectionActions active={anythingSelected}>
                <div className="d-flex align-items-center justify-content-center flex-wrap gap-2">
                    <div className="lead text-nowrap">
                        <strong className="text-emphasis me-1">{selectedTasks.length}</strong> selected
                    </div>
                    <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                        <Dropdown>
                            <Dropdown.Toggle
                                variant="secondary"
                                disabled={!anythingSelected || isTogglingState}
                                title="Set the status (enabled/disabled) of selected ongoing tasks"
                                className="rounded-pill"
                            >
                                {isTogglingState ? <Spinner size="sm" /> : <Icon icon="play" />} Set state
                            </Dropdown.Toggle>
                            <Dropdown.Menu>
                                <Dropdown.Item title="Enable" onClick={() => onTaskOperation("enable")}>
                                    <Icon icon="play" color="success" />
                                    <span>Enable</span>
                                </Dropdown.Item>
                                <Dropdown.Item title="Disable" onClick={() => onTaskOperation("disable")}>
                                    <Icon icon="stop" color="danger" />
                                    <span>Disable</span>
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>

                        <ButtonWithSpinner
                            variant="danger"
                            onClick={() => onTaskOperation("delete")}
                            className="rounded-pill flex-grow-0"
                            isSpinning={isDeleting}
                            icon="trash"
                        >
                            Delete
                        </ButtonWithSpinner>
                    </ButtonGroup>
                    <Button onClick={() => setSelectedTasks([])} variant="link">
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
