import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import React, { useState } from "react";
import Code from "components/common/Code";

interface IndexTermsPreviewModalProps {
    isOpen: boolean;
    toggleModal: () => void;
    termIndex: number;
    fieldTerms: string[];
}

export default function IndexTermsPreviewModal({
    isOpen,
    toggleModal,
    termIndex,
    fieldTerms,
}: IndexTermsPreviewModalProps) {
    const [currentIndex, setCurrentIndex] = useState<number>(termIndex);

    const term = fieldTerms[currentIndex] ?? "";

    const canNavigateToPreviousTerm = currentIndex > 0;
    const canNavigateToNextTerm = currentIndex < fieldTerms.length - 1;

    const navigateToPreviousTerm = () => {
        if (canNavigateToPreviousTerm) {
            setCurrentIndex(currentIndex - 1);
        }
    };

    const navigateToNextTerm = () => {
        if (canNavigateToNextTerm) {
            setCurrentIndex(currentIndex + 1);
        }
    };

    return (
        <Modal contentClassName="modal-border bulge-primary" show={isOpen}>
            <Modal.Header className="vstack gap-3" onCloseClick={toggleModal}>
                <div className="text-center">
                    <Icon icon="terms" color="primary" margin="me-0" className="fs-1" />
                </div>
                <div className="text-center lead">Indexing term value</div>
            </Modal.Header>
            <Modal.Body>
                <Code elementToCopy={term} code={term} language="plaintext" />
            </Modal.Body>
            <Modal.Footer className="mt-4 d-flex justify-content-between w-100">
                <div className="d-flex gap-2">
                    <Button
                        variant="secondary"
                        onClick={navigateToPreviousTerm}
                        disabled={!canNavigateToPreviousTerm}
                        className="rounded-pill"
                    >
                        <Icon icon="arrow-thin-left" />
                        Previous
                    </Button>
                    <Button
                        variant="secondary"
                        onClick={navigateToNextTerm}
                        disabled={!canNavigateToNextTerm}
                        className="rounded-pill"
                    >
                        Next
                        <Icon icon="arrow-thin-right" margin="ms-1" />
                    </Button>
                </div>
                <Button className="rounded-pill" variant="primary" onClick={toggleModal} type="button">
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
