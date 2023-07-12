import React, { useState } from "react";
import { Modal, ModalBody, ModalFooter, Button, CloseButton } from "reactstrap";
import {
    MultipleDatabaseLocationSelector,
    DatabaseActionContexts,
} from "components/common/MultipleDatabaseLocationSelector";
import { Icon } from "components/common/Icon";
import ActionContextUtils from "components/utils/actionContextUtils";

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
        <Modal isOpen wrapClassName="bs5">
            <ModalBody>
                <div className="text-right">
                    <CloseButton onClick={toggle} />
                </div>
                <div className="vstack align-items-center">
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
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" onClick={toggle}>
                    Cancel
                </Button>
                <Button color="danger" onClick={onSubmit}>
                    <Icon icon="reset" /> Restart
                </Button>
            </ModalFooter>
        </Modal>
    );
}
