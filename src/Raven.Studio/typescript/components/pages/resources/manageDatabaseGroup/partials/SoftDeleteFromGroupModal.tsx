import React from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import { useAsyncCallback } from "react-async-hook";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormGroup, FormLabel } from "components/common/Form";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseUtils from "components/utils/DatabaseUtils";
import useDeleteConfirmation from "hooks/useDeleteConfirmation";
import { useServices } from "hooks/useServices";
import {
    Consequence,
    CopyDataHint,
} from "components/pages/resources/manageDatabaseGroup/partials/DeleteFromGroupModalParts";

interface SoftDeleteFromGroupModalProps {
    db: DatabaseSharedInfo;
    nodeTag: string;
    onClose: () => void;
    onHardDeleteInstead: () => void;
}

export function SoftDeleteFromGroupModal({ db, nodeTag, onClose, onHardDeleteInstead }: SoftDeleteFromGroupModalProps) {
    const { databasesService } = useServices();
    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(true);

    const asyncSoftDelete = useAsyncCallback(() => databasesService.deleteDatabaseFromNode(db.name, [nodeTag], false), {
        onSuccess: onClose,
    });

    const deletedResource = DatabaseUtils.isSharded(db.name) ? (
        <>
            shard <strong>#{DatabaseUtils.shardNumber(db.name)}</strong>
        </>
    ) : (
        <>
            database <strong>{db.name}</strong>
        </>
    );

    return (
        <Modal show onHide={onClose} contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton className="vstack gap-4 pb-0" onCloseClick={onClose}>
                <div className="text-center">
                    <Icon icon="disconnected" color="danger" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">
                    Soft delete {deletedResource} from node <strong>{nodeTag}</strong>?
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-3">
                <div className="vstack gap-3 border border-secondary rounded p-3">
                    <ul className="list-unstyled vstack gap-2 mb-0">
                        <Consequence icon="disconnected">
                            <span>
                                Node <strong>{nodeTag}</strong> leaves the group and stops replicating.
                            </span>
                        </Consequence>
                        <Consequence icon="storage">
                            <span>Its database files stay on node {nodeTag}.</span>
                        </Consequence>
                        <Consequence icon="warning" iconColor="danger">
                            <div className="vstack gap-2 align-items-start">
                                <span>
                                    Other nodes stop keeping tombstones for node {nodeTag}.{" "}
                                    <strong>
                                        If it rejoins with these files, documents deleted in the meantime stay on it.
                                    </strong>
                                </span>
                                <Button
                                    variant="secondary"
                                    size="sm"
                                    className="rounded-pill"
                                    onClick={onHardDeleteInstead}
                                >
                                    Use Hard Delete instead
                                    <Icon icon="arrow-thin-right" margin="ms-1" />
                                </Button>
                            </div>
                        </Consequence>
                    </ul>
                    <hr className="m-0" />
                    <CopyDataHint databaseName={DatabaseUtils.shardGroupKey(db.name)} onNavigate={onClose} />
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
                    onClick={() => asyncSoftDelete.execute()}
                    isSpinning={asyncSoftDelete.loading}
                    disabled={!isConfirmed}
                >
                    Soft delete
                </ButtonWithSpinner>
            </Modal.Footer>
        </Modal>
    );
}
