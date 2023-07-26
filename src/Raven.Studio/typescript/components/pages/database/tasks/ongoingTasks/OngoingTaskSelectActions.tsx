import React from "react";
import genUtils from "common/generalUtils";
import { useAccessManager } from "components/hooks/useAccessManager";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import {
    ButtonGroup,
    UncontrolledDropdown,
    DropdownToggle,
    Spinner,
    DropdownMenu,
    DropdownItem,
    Button,
} from "reactstrap";
import { OngoingTaskToggleStateConfirmOperationType } from "./OngoingTaskToggleStateConfirm";

interface OngoingTaskSelectActionsProps {
    allTasks: string[];
    selectedTasks: string[];
    setSelectedTasks: (x: string[]) => void;
    onTaskOperation: (x: OngoingTaskToggleStateConfirmOperationType) => void;
    isTogglingState: boolean;
    isDeleting: boolean;
}

export default function OngoingTaskSelectActions(props: OngoingTaskSelectActionsProps) {
    const { allTasks, selectedTasks, setSelectedTasks, onTaskOperation, isTogglingState, isDeleting } = props;

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

            <SelectionActions active={anythingSelected && !isTogglingState}>
                <div className="d-flex align-items-center justify-content-center flex-wrap gap-2">
                    <div className="lead text-nowrap">
                        <strong className="text-emphasis me-1">{selectedTasks.length}</strong> selected
                    </div>
                    <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                        <UncontrolledDropdown>
                            <DropdownToggle
                                caret
                                disabled={!anythingSelected || isTogglingState}
                                title="Set the status (enabled/disabled) of selected databases"
                                className="rounded-pill"
                            >
                                {isTogglingState ? <Spinner size="sm" /> : <Icon icon="play" />} Set state
                            </DropdownToggle>
                            <DropdownMenu>
                                <DropdownItem title="Enable" onClick={() => onTaskOperation("enable")}>
                                    <Icon icon="play" color="success" />
                                    <span>Enable</span>
                                </DropdownItem>
                                <DropdownItem title="Disable" onClick={() => onTaskOperation("disable")}>
                                    <Icon icon="stop" color="danger" />
                                    <span>Disable</span>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>

                        <ButtonWithSpinner
                            color="danger"
                            onClick={() => onTaskOperation("delete")}
                            className="rounded-pill flex-grow-0"
                            isSpinning={isDeleting}
                            icon="trash"
                        >
                            Delete
                        </ButtonWithSpinner>
                    </ButtonGroup>
                    <Button onClick={() => setSelectedTasks([])} color="link">
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
