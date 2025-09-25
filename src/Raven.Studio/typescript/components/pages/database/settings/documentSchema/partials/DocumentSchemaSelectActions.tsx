import React from "react";
import genUtils from "common/generalUtils";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import { useAppSelector } from "components/store";
import { useDispatch } from "react-redux";
import { useEventsCollector } from "hooks/useEventsCollector";
import { documentSchemaSelectors } from "components/pages/database/settings/documentSchema/store/documentSchemaSliceSelectors";
import { documentSchemaActions } from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import useBoolean from "hooks/useBoolean";
import DocumentSchemaDeleteModal from "components/pages/database/settings/documentSchema/partials/DocumentSchemaDeleteModal";

export default function DocumentSchemaSelectActions() {
    const dispatch = useDispatch();
    const { reportEvent } = useEventsCollector();
    const { value: isDeleteModalOpen, toggle: toggleDeleteModal } = useBoolean(false);

    const allCollectionNames = useAppSelector(documentSchemaSelectors.allCollectionNames);
    const selectedCollectionNames = useAppSelector(documentSchemaSelectors.selectedCollectionNames);

    if (allCollectionNames.length === 0) {
        return null;
    }

    const selectionState = genUtils.getSelectionState(allCollectionNames, selectedCollectionNames);

    const toggleAll = () => {
        reportEvent("document-schema", "toggle-select-all");
        dispatch(documentSchemaActions.allSelectedCollectionNamesToggled());
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
                        <strong className="text-emphasis me-1">{selectedCollectionNames.length}</strong> selected
                    </div>
                    <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                        <Button variant="danger" onClick={toggleDeleteModal} className="rounded-pill flex-grow-0">
                            <Icon icon="trash" /> Delete
                        </Button>
                    </ButtonGroup>
                    <Button
                        onClick={() => dispatch(documentSchemaActions.allSelectedCollectionNamesToggled())}
                        variant="link"
                    >
                        Cancel
                    </Button>
                </div>
            </SelectionActions>
            {isDeleteModalOpen && (
                <DocumentSchemaDeleteModal
                    selectedCollectionNames={selectedCollectionNames}
                    onHide={toggleDeleteModal}
                />
            )}
        </div>
    );
}
