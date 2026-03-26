import React, { useState } from "react";
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
import useConfirm from "components/common/ConfirmDialog";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAppUrls } from "hooks/useAppUrls";
import { useViewSheet } from "components/common/splitView/ViewSheet";
import { ValidationSchemaViewSheetPanel } from "components/pages/database/settings/documentSchema/partials/ValidationSchemaViewSheetPanel";
import classNames from "classnames";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { ConditionalPopover } from "components/common/ConditionalPopover";

export interface OperationConfirm {
    type: DocumentSchemaOperationConfirmType;
    onConfirm: () => void;
    validators: DocumentSchemaValidatorConfig[];
}

export default function DocumentSchemaSelectActions() {
    const { forCurrentDatabase: urls } = useAppUrls();
    const dispatch = useDispatch();
    const { reportEvent } = useEventsCollector();
    const { value: isDeleteModalOpen, toggle: toggleDeleteModal } = useBoolean(false);
    const { value: isTogglingStatus, setTrue: setTogglingStatus, setFalse: unsetTogglingStatus } = useBoolean(false);
    const {
        value: isTogglingGlobalStatus,
        setTrue: setIsTogglingGlobalStatusTrue,
        setFalse: setIsTogglingGlobalStatusFalse,
    } = useBoolean(false);
    const confirm = useConfirm();
    const { open } = useViewSheet();
    const [operationConfirm, setOperationConfirm] = useState<OperationConfirm>(null);

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const allValidators = useAppSelector(documentSchemaSelectors.allValidators);
    const allCollectionNames = useAppSelector(documentSchemaSelectors.allCollectionNames);
    const selectedCollectionNames = useAppSelector(documentSchemaSelectors.selectedCollectionNames);
    const isGlobalDisabled = useAppSelector(documentSchemaSelectors.isGlobalDisabled);
    const selectionState = genUtils.getSelectionState(allCollectionNames, selectedCollectionNames);

    const hasSchemaValidation = useAppSelector(licenseSelectors.statusValue("HasSchemaValidation"));

    const handleOpenSheet = () => {
        const validators = allValidators.filter((v) => selectedCollectionNames.includes(v.Name));
        open({
            component: <ValidationSchemaViewSheetPanel validators={validators} />,
        });
        dispatch(documentSchemaActions.selectedCollectionNamesCleared());
    };

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

            const allUpdatedValidators = allValidators.map((validator) => {
                const updatedValidator = updatedValidators.find((uv) => uv.Name === validator.Name);
                return updatedValidator || validator;
            });

            await databasesService.saveSchemaValidation(
                databaseName,
                documentSchemaUtils.mapToSchemaValidationConfigurationDto(allUpdatedValidators, isGlobalDisabled)
            );

            updatedValidators.forEach((validator) => {
                dispatch(documentSchemaActions.validatorEdited({ originalName: validator.Name, validator }));
            });

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

    const handleGlobalStatusOperation = async (disabled: boolean) => {
        const isConfirmed = await confirm({
            title: disabled
                ? "Do you want to disable schema validation globally?"
                : "Do you want to enable schema validation globally?",
            message: disabled ? (
                <p>
                    This will disable schema validation for all collections in the database. Documents will no longer be
                    validated against their schemas.
                </p>
            ) : (
                <p>
                    This will <b>enable schema validation globally</b> for all collections in the database. Documents
                    will be validated against their defined schemas. If a schema has validation{" "}
                    <b>disabled individually</b>, you’ll need to <b>enable it manually</b>. The global setting does not
                    override per-schema configurations.
                </p>
            ),
            icon: disabled ? "stop" : "play",
            confirmText: disabled ? "Disable" : "Enable",
            actionColor: disabled ? "danger" : "success",
        });

        if (!isConfirmed) {
            return;
        }

        try {
            setIsTogglingGlobalStatusTrue();
            reportEvent("document-schema", disabled ? "global-disable" : "global-enable");

            await databasesService.saveSchemaValidation(
                databaseName,
                documentSchemaUtils.mapToSchemaValidationConfigurationDto(allValidators, disabled)
            );

            dispatch(documentSchemaActions.isGlobalDisabledToggled(disabled));
            dispatch(documentSchemaActions.validatorsSaved());
        } finally {
            setIsTogglingGlobalStatusFalse();
        }
    };

    return (
        <>
            <div
                className={classNames(
                    "position-relative d-flex w-100 align-items-center gap-2 mb-3",
                    allCollectionNames.length !== 0 ? "justify-content-between" : "justify-content-end"
                )}
            >
                {allCollectionNames.length !== 0 && (
                    <div>
                        <Checkbox
                            selected={selectionState === "AllSelected"}
                            indeterminate={selectionState === "SomeSelected"}
                            toggleSelection={toggleAll}
                            color="primary"
                            disabled={!hasSchemaValidation}
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
                                    <Button onClick={handleOpenSheet} className="rounded-pill flex-grow-0">
                                        <Icon icon="rocket" />
                                        Run test
                                    </Button>
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
                                            <Dropdown.Item
                                                title="Enable"
                                                onClick={() => handleStatusOperation("enable")}
                                            >
                                                <Icon icon="play" color="success" />
                                                <span>Enable</span>
                                            </Dropdown.Item>
                                            <Dropdown.Item
                                                title="Disable"
                                                onClick={() => handleStatusOperation("disable")}
                                            >
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
                )}
                <div className="d-flex gap-2 align-items-center">
                    <ConditionalPopover
                        conditions={{
                            isActive: !hasSchemaValidation,
                            message: <FeatureNotAvailableInYourLicensePopoverBody />,
                        }}
                    >
                        <a
                            className={classNames("btn btn-secondary rounded-pill", {
                                disabled: !hasSchemaValidation,
                            })}
                            href={urls.documentSchemaPlayground()}
                            title="Open the playground to test sample schemas against existing documents"
                        >
                            <Icon icon="rocket" />
                            Schema Playground
                        </a>
                    </ConditionalPopover>
                    {allCollectionNames.length !== 0 && (
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasSchemaValidation,
                                message: <FeatureNotAvailableInYourLicensePopoverBody />,
                            }}
                        >
                            <ButtonWithSpinner
                                variant={isGlobalDisabled ? "success" : "secondary"}
                                onClick={() => handleGlobalStatusOperation(!isGlobalDisabled)}
                                isSpinning={isTogglingGlobalStatus}
                                disabled={!hasSchemaValidation}
                                className="rounded-pill"
                            >
                                <Icon icon={isGlobalDisabled ? "play" : "stop"} />
                                {isGlobalDisabled ? "Enable" : "Disable"} Schema Validation
                            </ButtonWithSpinner>
                        </ConditionalPopover>
                    )}
                </div>
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
