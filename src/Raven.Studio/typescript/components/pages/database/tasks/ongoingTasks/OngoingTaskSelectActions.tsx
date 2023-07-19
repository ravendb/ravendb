import React from "react";
import genUtils from "common/generalUtils";
import { OngoingTaskSharedInfo } from "components/models/tasks";
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

interface OngoingTaskSelectActionsProps {
    allTasks: OngoingTaskSharedInfo[];
    selectedTasks: OngoingTaskSharedInfo[];
    setSelectedTasks: (x: OngoingTaskSharedInfo[]) => void;
}

export default function OngoingTaskSelectActions(props: OngoingTaskSelectActionsProps) {
    const { allTasks, selectedTasks, setSelectedTasks } = props;

    const { isOperatorOrAbove } = useAccessManager();

    const anythingSelected = selectedTasks.length > 0;
    const selectionState = genUtils.getSelectionState(
        allTasks.map((x) => x.taskName),
        selectedTasks.map((x) => x.taskName)
    );

    const toggleSelectAll = () => {
        if (selectionState === "Empty") {
            setSelectedTasks([...selectedTasks, ...allTasks]);
        } else {
            setSelectedTasks(selectedTasks.filter((x) => !allTasks.map((x) => x.taskName).includes(x.taskName)));
        }
    };

    const onToggleTasks = (enable: boolean) => {
        console.log("kalczur onToggleTasks", enable);
    };

    const onDelete = () => {
        console.log("kalczur onDelete");
    };

    // TODO kalczur
    const isTogglingState = false;
    const isDeleting = false;

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
                                <DropdownItem title="Enable" onClick={() => onToggleTasks(true)}>
                                    <Icon icon="play" />
                                    <span>Enable</span>
                                </DropdownItem>
                                <DropdownItem title="Disable" onClick={() => onToggleTasks(false)}>
                                    <Icon icon="stop" />
                                    <span>Disable</span>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>

                        {isOperatorOrAbove() && (
                            <ButtonWithSpinner
                                color="danger"
                                onClick={onDelete}
                                className="rounded-pill flex-grow-0"
                                isSpinning={isDeleting}
                                icon="trash"
                            >
                                Delete
                            </ButtonWithSpinner>
                        )}
                    </ButtonGroup>
                    <Button onClick={() => setSelectedTasks([])} color="link">
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
