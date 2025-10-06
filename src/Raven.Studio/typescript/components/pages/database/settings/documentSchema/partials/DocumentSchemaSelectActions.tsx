import React from "react";
import genUtils from "common/generalUtils";
import { Icon } from "components/common/Icon";
import { Checkbox } from "components/common/Checkbox";
import { SelectionActions } from "components/common/SelectionActions";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import Dropdown from "react-bootstrap/Dropdown";
import Spinner from "react-bootstrap/Spinner";
import { useAppSelector } from "components/store";
import { useDispatch } from "react-redux";
import { useEventsCollector } from "hooks/useEventsCollector";
import { documentSchemaSelectors } from "components/pages/database/settings/documentSchema/store/documentSchemaSliceSelectors";
import {
    documentSchemaActions,
    DocumentSchemaValidatorConfig,
} from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import useBoolean from "hooks/useBoolean";
import DocumentSchemaDeleteModal from "components/pages/database/settings/documentSchema/partials/DocumentSchemaDeleteModal";
import DocumentSchemaOperationConfirm, {
    DocumentSchemaOperationConfirmType,
} from "components/pages/database/settings/documentSchema/partials/DocumentSchemaOperationConfirm";
import { useServices } from "hooks/useServices";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";

interface OperationConfirm {
    type: DocumentSchemaOperationConfirmType;
    onConfirm: () => void;
    validators: DocumentSchemaValidatorConfig[];
}

export default function DocumentSchemaSelectActions() {
    const dispatch = useDispatch();
    const { reportEvent } = useEventsCollector();
    const { value: isDeleteModalOpen, toggle: toggleDeleteModal } = useBoolean(false);
    const { value: isTogglingStatus, setTrue: setTogglingStatus, setFalse: unsetTogglingStatus } = useBoolean(false);
    const {
        setTrue: setTogglingGlobalStatus,
        setFalse: unsetTogglingGlobalStatus,
    } = useBoolean(false);
    const [operationConfirm, setOperationConfirm] = React.useState<OperationConfirm>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const allValidators = useAppSelector(documentSchemaSelectors.allValidators);
    const allCollectionNames = useAppSelector(documentSchemaSelectors.allCollectionNames);
    const selectedCollectionNames = useAppSelector(documentSchemaSelectors.selectedCollectionNames);
    const globalDisabled = useAppSelector(documentSchemaSelectors.globalDisabled);

    if (allCollectionNames.length === 0) {
        return null;
    }

    const selectionState = genUtils.getSelectionState(allCollectionNames, selectedCollectionNames);

    const toggleAll = () => {
        reportEvent("document-schema", "toggle-select-all");
        dispatch(documentSchemaActions.allSelectedCollectionNamesToggled());
    };

    const handleBulkStatusToggle = async (disabled: boolean) => {
        try {
            setTogglingStatus();
            reportEvent("document-schema", disabled ? "bulk-disable" : "bulk-enable");

            const selectedValidators = allValidators.filter((v) => selectedCollectionNames.includes(v.Name));
            const updatedValidators: DocumentSchemaValidatorConfig[] = selectedValidators.map((validator) => ({
                ...validator,
                Disabled: disabled,
            }));

            updatedValidators.forEach((validator) => {
                dispatch(documentSchemaActions.validatorEdited({ originalName: validator.Name, validator }));
            });

            const allUpdatedValidators = allValidators.map((validator) => {
                const updatedValidator = updatedValidators.find((uv) => uv.Name === validator.Name);
                return updatedValidator || validator;
            });

            await databasesService.saveSchemaValidation(
                databaseName,
                documentSchemaUtils.mapToSchemaValidationConfigurationDto(allUpdatedValidators, globalDisabled)
            );

            dispatch(documentSchemaActions.validatorsSaved());
        } finally {
            unsetTogglingStatus();
        }
    };

    const handleStatusOperation = (type: DocumentSchemaOperationConfirmType) => {
        const selectedValidators = allValidators.filter((v) => selectedCollectionNames.includes(v.Name));

        setOperationConfirm({
            type,
            onConfirm: () => handleBulkStatusToggle(type === "disable"),
            validators: selectedValidators,
        });
    };

    const handleGlobalStatusToggle = async (disabled: boolean) => {
        try {
            setTogglingGlobalStatus();
            reportEvent("document-schema", disabled ? "global-disable" : "global-enable");

            dispatch(documentSchemaActions.globalDisabledToggled(disabled));

            await databasesService.saveSchemaValidation(
                databaseName,
                documentSchemaUtils.mapToSchemaValidationConfigurationDto(allValidators, disabled)
            );

            dispatch(documentSchemaActions.validatorsSaved());
        } finally {
            unsetTogglingGlobalStatus();
        }
    };

    const handleGlobalStatusOperation = (type: DocumentSchemaOperationConfirmType) => {
        setOperationConfirm({
            type,
            onConfirm: () => handleGlobalStatusToggle(type === "disable"),
            validators: allValidators,
        });
    };

    return (
        <>
            <div className="position-relative d-flex w-100 justify-content-between align-items-center gap-2">
                <div>
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
                                <strong className="text-emphasis me-1">{selectedCollectionNames.length}</strong>{" "}
                                selected
                            </div>
                            <ButtonGroup className="gap-2 flex-wrap justify-content-center">
                                <Dropdown>
                                    <Dropdown.Toggle
                                        variant="secondary"
                                        disabled={selectionState === "Empty" || isTogglingStatus}
                                        title="Set the status (enabled/disabled) of selected document schemas"
                                        className="rounded-pill"
                                    >
                                        {isTogglingStatus ? <Spinner size="sm" /> : <Icon icon="play" />} Set state
                                    </Dropdown.Toggle>
                                    <Dropdown.Menu>
                                        <Dropdown.Item title="Enable" onClick={() => handleStatusOperation("enable")}>
                                            <Icon icon="play" color="success" />
                                            <span>Enable</span>
                                        </Dropdown.Item>
                                        <Dropdown.Item title="Disable" onClick={() => handleStatusOperation("disable")}>
                                            <Icon icon="stop" color="danger" />
                                            <span>Disable</span>
                                        </Dropdown.Item>
                                    </Dropdown.Menu>
                                </Dropdown>

                                <Button
                                    variant="danger"
                                    onClick={toggleDeleteModal}
                                    className="rounded-pill flex-grow-0"
                                >
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
                </div>
                {globalDisabled ? (
                    <Button variant="success" onClick={() => handleGlobalStatusOperation("enable")}>
                        Enable Schema Validation for All Collections
                    </Button>
                ) : (
                    <Button variant="secondary" onClick={() => handleGlobalStatusOperation("disable")}>
                        Disable Schema Validation for All Collections
                    </Button>
                )}
            </div>
            {isDeleteModalOpen && (
                <DocumentSchemaDeleteModal
                    selectedCollectionNames={selectedCollectionNames}
                    onHide={toggleDeleteModal}
                />
            )}
            {operationConfirm && (
                <DocumentSchemaOperationConfirm {...operationConfirm} toggle={() => setOperationConfirm(null)} />
            )}
        </>
    );
}
