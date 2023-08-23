import React from "react";
import genUtils from "common/generalUtils";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import { ButtonGroup, UncontrolledDropdown, DropdownToggle, DropdownMenu, DropdownItem, Button } from "reactstrap";
import { useAppSelector } from "components/store";
import { documentRevisionsActions, documentRevisionsSelectors } from "./store/documentRevisionsSlice";
import { useDispatch } from "react-redux";
import { useEventsCollector } from "components/hooks/useEventsCollector";

export default function DocumentRevisionsSelectActions() {
    const dispatch = useDispatch();
    const { reportEvent } = useEventsCollector();

    const allConfigsNames = useAppSelector(documentRevisionsSelectors.allConfigsNames);
    const selectedConfigsNames = useAppSelector(documentRevisionsSelectors.selectedConfigNames);

    if (allConfigsNames.length === 0) {
        return null;
    }

    const selectionState = genUtils.getSelectionState(allConfigsNames, selectedConfigsNames);

    const toggleAll = () => {
        reportEvent("revisions", "toggle-select-all");
        dispatch(documentRevisionsActions.toggleAllSelectedConfigNames());
    };

    return (
        <div className="position-relative">
            <Checkbox
                selected={selectionState === "AllSelected"}
                indeterminate={selectionState === "SomeSelected"}
                toggleSelection={toggleAll}
                color="primary"
                title="Select all or none"
                size="lg"
            >
                <span className="small-label">Select All</span>
            </Checkbox>

            <SelectionActions active={selectionState !== "Empty"}>
                <div className="d-flex align-items-center justify-content-center flex-wrap gap-2">
                    <div className="lead text-nowrap">
                        <strong className="text-emphasis me-1">{selectedConfigsNames.length}</strong> selected
                    </div>
                    <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                        <UncontrolledDropdown>
                            <DropdownToggle
                                caret
                                title="Set the status (enabled/disabled) of selected revisions"
                                className="rounded-pill"
                            >
                                <Icon icon="play" /> Set state
                            </DropdownToggle>
                            <DropdownMenu>
                                <DropdownItem
                                    title="Enable"
                                    onClick={() => dispatch(documentRevisionsActions.enableSelectedConfigs())}
                                >
                                    <Icon icon="play" color="success" />
                                    <span>Enable</span>
                                </DropdownItem>
                                <DropdownItem
                                    title="Disable"
                                    onClick={() => dispatch(documentRevisionsActions.disableSelectedConfigs())}
                                >
                                    <Icon icon="stop" color="danger" />
                                    <span>Disable</span>
                                </DropdownItem>
                            </DropdownMenu>
                        </UncontrolledDropdown>

                        <Button
                            color="danger"
                            onClick={() => dispatch(documentRevisionsActions.deleteSelectedConfigs())}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="trash" /> Delete
                        </Button>
                    </ButtonGroup>
                    <Button
                        onClick={() => dispatch(documentRevisionsActions.toggleAllSelectedConfigNames())}
                        color="link"
                    >
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
