import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import React from "react";
import {
    documentSchemaActions,
    DocumentSchemaValidatorConfig,
} from "components/pages/database/settings/documentSchema/store/documentSchemaSlice";
import { useAppDispatch, useAppSelector } from "components/store";
import { documentSchemaSelectors } from "components/pages/database/settings/documentSchema/store/documentSchemaSliceSelectors";
import { useAsyncCallback } from "react-async-hook";
import { documentSchemaUtils } from "components/pages/database/settings/documentSchema/documentSchemaUtils";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "hooks/useServices";

interface DocumentSchemaDeleteModalProps {
    collectionName?: string;
    selectedCollectionNames?: string[];
    onHide: () => void;
}

function getNamesToDelete(selectedCollectionNames?: string[], collectionName?: string): string[] {
    if (selectedCollectionNames && selectedCollectionNames.length > 0) {
        return selectedCollectionNames;
    }

    if (collectionName) {
        return [collectionName];
    }

    return [];
}

export default function DocumentSchemaDeleteModal({
    collectionName,
    selectedCollectionNames,
    onHide,
}: DocumentSchemaDeleteModalProps) {
    const { databasesService } = useServices();
    const dispatch = useAppDispatch();
    const validatorsAll = useAppSelector(documentSchemaSelectors.allValidators);
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const asyncSaveValidators = useAsyncCallback(async (items: DocumentSchemaValidatorConfig[]) => {
        await databasesService.saveSchemaValidation(
            databaseName,
            documentSchemaUtils.mapToSchemaValidationConfigurationDto(items)
        );
        dispatch(documentSchemaActions.validatorsSaved());
    });

    const namesToDelete = getNamesToDelete(selectedCollectionNames, collectionName);

    const isBulk = namesToDelete.length > 1;

    const onDelete = async () => {
        const next = validatorsAll.filter((v) => !namesToDelete.includes(v.Name));
        await asyncSaveValidators.execute(next);
        if (isBulk) {
            dispatch(documentSchemaActions.selectedValidatorsDeleted());
        } else if (namesToDelete.length === 1) {
            dispatch(documentSchemaActions.validatorDeleted(namesToDelete[0]));
        }
        onHide();
    };

    return (
        <Modal show centered contentClassName="modal-border bulge-danger">
            <Modal.Header className="vstack gap-3 pb-0" onCloseClick={onHide}>
                <div className="text-center">
                    <Icon icon="warning" color="danger" margin="m-0" className="fs-1" />
                </div>
                <div className="text-center lead">
                    {isBulk ? (
                        <>
                            Delete schemas for <b>{namesToDelete.length}</b> collections?
                        </>
                    ) : (
                        <>
                            Delete schema for <b>{namesToDelete[0]}</b>?
                        </>
                    )}
                </div>
            </Modal.Header>
            <Modal.Body className="pb-0 vstack gap-3">
                <div className="text-center">
                    {isBulk ? (
                        <>
                            This action will permanently delete the JSON schemas for the selected collections. This
                            cannot be undone.
                            <br /> <p className="mt-3 mb-0">Are you sure you want to proceed?</p>
                        </>
                    ) : (
                        <>
                            This action will permanently delete the JSON schema for the {namesToDelete[0]} collection.
                            This cannot be undone. <br /> <p className="mt-3 mb-0">Are you sure you want to proceed?</p>
                        </>
                    )}
                </div>
            </Modal.Body>
            <Modal.Footer className="mt-4">
                <Button className="link-muted" variant="link" onClick={onHide} type="button">
                    Close
                </Button>
                <ButtonWithSpinner
                    className="rounded-pill"
                    variant="danger"
                    icon="trash"
                    isSpinning={asyncSaveValidators.loading}
                    onClick={onDelete}
                    title={isBulk ? "Delete schemas" : "Delete schema"}
                >
                    {isBulk ? "Delete schemas" : "Delete schema"}
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
