import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import {
    MultipleDatabaseLocationSelector,
    DatabaseActionContexts,
} from "components/common/MultipleDatabaseLocationSelector";
import { Icon } from "components/common/Icon";
import ActionContextUtils from "components/utils/actionContextUtils";
import Modal from "components/common/Modal";

interface BulkDatabaseResetConfirm {
    dbName: string;
    localNodeTag: string;
    allActionContexts: DatabaseActionContexts[];
    toggleConfirmation: () => void;
    onConfirm: (locations: databaseLocationSpecifier[]) => void;
}

export default function BulkDatabaseResetConfirm({
    dbName,
    localNodeTag,
    allActionContexts,
    toggleConfirmation: toggle,
    onConfirm,
}: BulkDatabaseResetConfirm) {
    const [actionContexts, setActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const onSubmit = () => {
        onConfirm(actionContexts);
        toggle();
    };

    return (
        <Modal show>
            <Modal.Header className="vstack gap-2 pb-0" onCloseClick={toggle}>
                <h3>
                    Restart{" "}
                    <small className="d-inline-block bg-faded-primary rounded-pill px-2 py-1 mx-1">
                        <Icon icon="database" />
                        {dbName}
                    </small>
                    {!ActionContextUtils.showContextSelector(allActionContexts) && (
                        <>
                            on node{" "}
                            <small className="text-node">
                                <Icon icon="node" margin="m-0" /> <strong>{localNodeTag}</strong>
                            </small>
                        </>
                    )}{" "}
                    ?
                </h3>
            </Modal.Header>
            <Modal.Body className="pt-0">
                <div className="vstack align-items-center">
                    {ActionContextUtils.showContextSelector(allActionContexts) && (
                        <div>
                            <p>Select restart context:</p>
                            <MultipleDatabaseLocationSelector
                                allActionContexts={allActionContexts}
                                selectedActionContexts={actionContexts}
                                setSelectedActionContexts={setActionContexts}
                            />
                        </div>
                    )}
                </div>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" className="link-muted" onClick={toggle}>
                    Cancel
                </Button>
                <Button variant="danger" onClick={onSubmit}>
                    <Icon icon="reset" /> Restart
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
