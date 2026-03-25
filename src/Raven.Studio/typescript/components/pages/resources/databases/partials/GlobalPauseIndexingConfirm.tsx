import Modal from "components/common/Modal";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import { Icon } from "components/common/Icon";
import { useState } from "react";
import Button from "react-bootstrap/Button";

interface GlobalPauseIndexingConfirmProps {
    toggle: () => void;
    onConfirm: (contextPoints: DatabaseActionContexts[]) => void;
    allActionContexts: DatabaseActionContexts[];
}

export function GlobalPauseIndexingConfirm(props: GlobalPauseIndexingConfirmProps) {
    const { allActionContexts, toggle, onConfirm } = props;

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const handlePause = () => {
        onConfirm(selectedActionContexts);
        toggle();
    };

    return (
        <Modal show scrollable onHide={toggle} contentClassName="modal-border bulge-warning">
            <Modal.Header className="vstack gap-4" onCloseClick={toggle}>
                <div className="text-center">
                    <Icon icon="index" color="warning" addon="pause" className="fs-1" margin="m-0" />
                </div>
                <div className="text-center lead">You&apos;re about to pause indexing until restart</div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4 pt-0">
                <p>
                    Are you sure you want to pause indexing until the server restarts? This will affect both existing
                    indexes and any new indexes created in this database during that time.
                </p>
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
                <Button variant="warning" onClick={handlePause} className="rounded-pill">
                    <Icon icon="pause" /> Pause
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
