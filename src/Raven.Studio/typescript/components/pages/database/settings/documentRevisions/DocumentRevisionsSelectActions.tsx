import React from "react";
import genUtils from "common/generalUtils";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import { useAppSelector } from "components/store";
import { documentRevisionsActions } from "./store/documentRevisionsSlice";
import { documentRevisionsSelectors } from "./store/documentRevisionsSliceSelectors";
import { useDispatch } from "react-redux";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import Dropdown from "react-bootstrap/Dropdown";
import { CustomDropdownToggle } from "components/common/Dropdown";

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
        dispatch(documentRevisionsActions.allSelectedConfigNamesToggled());
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
                        <Dropdown>
                            <Dropdown.Toggle
                                as={CustomDropdownToggle}
                                variant="secondary"
                                isCaretHidden
                                title="Set the status (enabled/disabled) of selected revisions"
                                className="rounded-pill"
                            >
                                <Icon icon="play" /> Set state
                            </Dropdown.Toggle>
                            <Dropdown.Menu>
                                <Dropdown.Item
                                    title="Enable"
                                    onClick={() => dispatch(documentRevisionsActions.selectedConfigsEnabled())}
                                >
                                    <Icon icon="play" color="success" />
                                    <span>Enable</span>
                                </Dropdown.Item>
                                <Dropdown.Item
                                    title="Disable"
                                    onClick={() => dispatch(documentRevisionsActions.selectedConfigsDisabled())}
                                >
                                    <Icon icon="stop" color="danger" />
                                    <span>Disable</span>
                                </Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>

                        <Button
                            variant="danger"
                            onClick={() => dispatch(documentRevisionsActions.selectedConfigsDeleted())}
                            className="rounded-pill flex-grow-0"
                        >
                            <Icon icon="trash" /> Delete
                        </Button>
                    </ButtonGroup>
                    <Button
                        onClick={() => dispatch(documentRevisionsActions.allSelectedConfigNamesToggled())}
                        variant="link"
                    >
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
        </div>
    );
}
