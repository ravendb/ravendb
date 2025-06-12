import Modal from "components/common/Modal";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import { Icon } from "components/common/Icon";
import { useState } from "react";
import Button from "react-bootstrap/Button";

interface GlobalResumeIndexingConfirmProps {
    toggle: () => void;
    onConfirm: (contextPoints: DatabaseActionContexts[]) => void;
    allActionContexts: DatabaseActionContexts[];
}

export function GlobalResumeIndexingConfirm(props: GlobalResumeIndexingConfirmProps) {
    const { allActionContexts, toggle, onConfirm } = props;

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const handleResume = () => {
        onConfirm(selectedActionContexts);
        toggle();
    };

    return (
        <Modal show scrollable onHide={toggle} contentClassName="modal-border bulge-success">
            <Modal.Header className="vstack gap-4" onCloseClick={toggle}>
                <div className="text-center">
                    <Icon icon="index" color="success" addon="play" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">You&apos;re about to resume indexing</div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4">
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
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={toggle} className="link-muted">
                    Cancel
                </Button>
                <Button variant="success" onClick={handleResume} className="rounded-pill">
                    <Icon icon="play" /> Resume
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
