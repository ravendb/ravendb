import React from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { useAsyncCallback } from "react-async-hook";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormGroup, FormLabel } from "components/common/Form";
import { DatabaseSharedInfo } from "components/models/databases";
import useDeleteConfirmation from "hooks/useDeleteConfirmation";
import { useServices } from "hooks/useServices";
import {
    Consequence,
    CopyDataHint,
} from "components/pages/resources/manageDatabaseGroup/partials/DeleteFromGroupModalParts";

interface DeleteLastNodeModalProps {
    db: DatabaseSharedInfo;
    nodeTag: string;
    hardDelete: boolean;
    onClose: () => void;
}

export function DeleteLastNodeModal({ db, nodeTag, hardDelete, onClose }: DeleteLastNodeModalProps) {
    const { databasesService } = useServices();
    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(true);

    const asyncDelete = useAsyncCallback(
        () => databasesService.deleteDatabaseFromNode(db.name, [nodeTag], hardDelete),
        { onSuccess: onClose }
    );

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton className="vstack gap-4 pb-0" onCloseClick={onClose}>
                <div className="text-center">
                    <Icon icon="trash" color="danger" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">
                    Delete database <strong>{db.name}</strong>?
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-3">
                <div className="vstack gap-3 border border-secondary rounded p-3">
                    <ul className="list-unstyled vstack gap-2 mb-0">
                        <Consequence icon="warning" iconColor="danger">
                            <span>
                                Node <strong>{nodeTag}</strong> is the only node in the group, so{" "}
                                <strong>database {db.name} is deleted from the cluster.</strong>
                            </span>
                        </Consequence>
                        {hardDelete ? (
                            <Consequence icon="trash" iconColor="danger">
                                <strong>
                                    The database files are deleted from node {nodeTag}. This can&apos;t be undone.
                                </strong>
                            </Consequence>
                        ) : (
                            <Consequence icon="storage">
                                <span>The database files stay on node {nodeTag}.</span>
                            </Consequence>
                        )}
                    </ul>
                    <hr className="m-0" />
                    <CopyDataHint databaseName={db.name} onNavigate={onClose} />
                </div>
                <FormGroup marginClass="mb-0">
                    <FormLabel className="fw-bold">Type DELETE to confirm</FormLabel>
                    <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                </FormGroup>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={onClose} className="link-muted">
                    Cancel
                </Button>
                <ButtonWithSpinner
                    variant="danger"
                    className="rounded-pill"
                    onClick={() => asyncDelete.execute()}
                    isSpinning={asyncDelete.loading}
                    disabled={!isConfirmed}
                >
                    {hardDelete ? "Hard delete" : "Soft delete"}
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
