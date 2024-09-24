import React, { useState } from "react";
import { Button, Modal, ModalBody, ModalFooter } from "reactstrap";
import { Icon } from "components/common/Icon";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import RichAlert from "components/common/RichAlert";

interface ConfirmSwapSideBySideIndexProps {
    indexName: string;
    toggle: () => void;
    allActionContexts: DatabaseActionContexts[];
    onConfirm: (contexts: DatabaseActionContexts[]) => void;
}

export function ConfirmSwapSideBySideIndex(props: ConfirmSwapSideBySideIndexProps) {
    const { indexName, toggle, onConfirm, allActionContexts } = props;

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const onSubmit = () => {
        onConfirm(selectedActionContexts);
        toggle();
    };

    return (
        <Modal isOpen toggle={toggle} wrapClassName="bs5" centered contentClassName="modal-border bulge-warning">
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="index" color="warning" addon="swap" className="fs-1" margin="m-0" />
                </div>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggle} />
                </div>
                <div className="text-center lead">
                    You&apos;re about to <span className="text-warning">swap</span> following index
                </div>
                <span className="text-center bg-faded-primary py-1 px-3 w-fit-content rounded-pill mx-auto">
                    <Icon icon="index" />
                    {indexName}
                </span>
                <RichAlert variant="warning">
                    Clicking <strong>Swap Now</strong> will immediately replace the current index definition with the
                    replacement index.
                </RichAlert>
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
                <Button color="link" onClick={toggle} className="link-muted">
                    Cancel
                </Button>
                <Button color="warning" onClick={onSubmit} className="rounded-pill">
                    <Icon icon="swap" />
                    Swap Now
                </Button>
            </ModalFooter>
        </Modal>
    );
}
