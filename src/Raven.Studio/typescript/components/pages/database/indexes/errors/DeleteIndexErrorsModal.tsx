import React from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { useAsync } from "react-async-hook";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import RichAlert from "components/common/RichAlert";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { Switch } from "components/common/Checkbox";
import { FormGroup, FormLabel } from "components/common/Form";
import useDeleteConfirmation from "hooks/useDeleteConfirmation";
import studioSettings from "common/settings/studioSettings";
import messagePublisher from "common/messagePublisher";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";

interface DeleteIndexErrorsModalProps {
    location: databaseLocationSpecifier;
    selectedIndexNames: string[];
    isDeleting: boolean;
    onConfirm: () => void;
    onClose: () => void;
}

export default function DeleteIndexErrorsModal({
    location,
    selectedIndexNames,
    isDeleting,
    onConfirm,
    onClose,
}: DeleteIndexErrorsModalProps) {
    const asyncGlobalSettings = useAsync(async () => await studioSettings.default.globalSettings(), []);

    const isRequireTypedConfirm =
        asyncGlobalSettings.result?.isRequireTypedConfirmationToDeleteIndexErrors.getValue() ?? true;

    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(isRequireTypedConfirm);

    const toggleIsRequireTypedConfirm = async () => {
        if (!asyncGlobalSettings.result) {
            messagePublisher.reportError("Failed to load studio global settings");
            return;
        }

        asyncGlobalSettings.result.isRequireTypedConfirmationToDeleteIndexErrors.setValue(!isRequireTypedConfirm);
        await asyncGlobalSettings.execute();
    };

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton onCloseClick={onClose} className="pb-0">
                <h3>
                    <Icon icon="trash" color="danger" />
                    <span>
                        Delete errors for <Icon icon="node" color="node" margin="m-0" />{" "}
                        <strong>{location.nodeTag}</strong>
                        {location.shardNumber != null && (
                            <>
                                {" "}
                                <Icon icon="shard" color="shard" margin="m-0" />{" "}
                                <strong>#{location.shardNumber}</strong>
                            </>
                        )}
                        ?
                    </span>
                </h3>
            </Modal.Header>
            <Modal.Body className="pt-0">
                <SelectedIndexesInfo selectedIndexNames={selectedIndexNames} />
                <RichAlert variant="info" className="mt-3">
                    While the current indexing errors will be cleared, an index in an <b>Error state</b> will not be set
                    back to the <b>Normal</b> state.
                </RichAlert>
                {isRequireTypedConfirm && (
                    <FormGroup className="mt-3">
                        <FormLabel className="fw-bold">Type DELETE to confirm</FormLabel>
                        <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                    </FormGroup>
                )}
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between">
                <Switch
                    selected={isRequireTypedConfirm}
                    toggleSelection={toggleIsRequireTypedConfirm}
                    disabled={!asyncGlobalSettings.result}
                    color="primary"
                >
                    Require typed confirmation
                </Switch>
                <div className="hstack gap-2 flex-grow-1 justify-content-end">
                    <Button variant="link" onClick={onClose} className="link-muted">
                        Cancel
                    </Button>
                    <ButtonWithSpinner
                        isSpinning={isDeleting}
                        variant="danger"
                        onClick={onConfirm}
                        className="rounded-pill"
                        disabled={!isConfirmed || isDeleting}
                    >
                        Delete
                    </ButtonWithSpinner>
                </div>
            </Modal.Footer>
        </Modal>
    );
}

interface SelectedIndexesInfoProps {
    selectedIndexNames: string[];
}

function SelectedIndexesInfo({ selectedIndexNames }: SelectedIndexesInfoProps) {
    if (selectedIndexNames.length === 0) {
        return (
            <p>
                Errors will be deleted for <strong>ALL</strong> indexes. <br />
                To delete errors for <b>specific indexes</b>, select them in the dropdown.
            </p>
        );
    }

    return (
        <div>
            You&#39;re deleting errors for <b>{selectedIndexNames.length}</b>{" "}
            {pluralizeHelpers.pluralize(selectedIndexNames.length, "index", "indexes", true)}:
            <ul>
                {selectedIndexNames.map((indexName) => (
                    <li key={indexName} title={indexName} className="text-truncate">
                        <b>{indexName}</b>
                    </li>
                ))}
            </ul>
        </div>
    );
}
