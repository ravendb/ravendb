import React, { useState } from "react";
import { Alert, Button, Modal, ModalBody, ModalFooter } from "reactstrap";
import { Icon } from "components/common/Icon";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";

interface ConfirmResetIndexProps {
    indexName: string;
    allActionContexts: DatabaseActionContexts[];
    mode?: Raven.Client.Documents.Indexes.IndexResetMode;
    closeConfirm: () => void;
    onConfirm: (contexts: DatabaseActionContexts[]) => void;
}

export function ConfirmResetIndex(props: ConfirmResetIndexProps) {
    const { indexName, mode, allActionContexts, onConfirm, closeConfirm } = props;

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const onSubmit = () => {
        onConfirm(selectedActionContexts);
        closeConfirm();
    };

    return (
        <Modal isOpen toggle={closeConfirm} wrapClassName="bs5" centered contentClassName="modal-border bulge-warning">
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="index" color="warning" addon="reset-index" className="fs-1" margin="m-0" />
                </div>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={closeConfirm} />
                </div>
                <div className="text-center lead">
                    You&apos;re about to <span className="text-warning">reset</span> following index
                </div>
                <span className="d-flex align-items-center word-break bg-faded-primary py-1 px-3 w-fit-content rounded-pill mx-auto">
                    <Icon icon="index" />
                    {indexName}
                </span>
                <Alert color="warning">
                    <small>
                        <strong>Reset</strong> will remove all existing indexed data
                        {ActionContextUtils.showContextSelector(allActionContexts) ? (
                            <span> from the selected context.</span>
                        ) : (
                            <span> from node {allActionContexts[0].nodeTag}.</span>
                        )}
                    </small>
                    <br />
                    <small>All items matched by the index definition will be re-indexed.</small>
                </Alert>
                {mode && (
                    <Alert color="info">
                        <strong>Reset mode: </strong>
                        {mode === "InPlace" && <span>In place</span>}
                        {mode === "SideBySide" && <span>Side by side</span>}
                    </Alert>
                )}
                {ActionContextUtils.showContextSelector(allActionContexts) && (
                    <div>
                        <h4>Select context</h4>
                        <MultipleDatabaseLocationSelector
                            allActionContexts={allActionContexts}
                            selectedActionContexts={selectedActionContexts}
                            setSelectedActionContexts={setSelectedActionContexts}
                        />
                    </div>
                )}
            </ModalBody>
            <ModalFooter>
                <Button color="link" onClick={closeConfirm} className="link-muted">
                    Cancel
                </Button>
                <Button
                    color="warning"
                    onClick={onSubmit}
                    className="rounded-pill"
                    disabled={selectedActionContexts.length === 0}
                >
                    <Icon icon="reset-index" />
                    Reset
                </Button>
            </ModalFooter>
        </Modal>
    );
}
