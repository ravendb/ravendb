import { Button, Modal, ModalBody, ModalFooter } from "reactstrap";
import { Icon } from "components/common/Icon";
import React, { useEffect, useState } from "react";
import Code from "components/common/Code";

interface IndexTermsPreviewModalProps {
    isOpen: boolean;
    toggleModal: () => void;
    termIndex: number;
    fieldTerms: string[];
}

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

    useEffect(() => {
        setCurrentIndex(termIndex);
    }, [termIndex]);

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
        <Modal centered contentClassName="modal-border bulge-primary" isOpen={isOpen} wrapClassName="bs5">
            <ModalBody className="pb-0 vstack gap-3">
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggleModal} />
                </div>
                <div className="text-center">
                    <Icon icon="terms" color="primary" margin="me-0" className="fs-1" />
                </div>
                <div className="text-center lead">Indexing term value</div>
                <Code elementToCopy={term} code={term} language="plaintext" />
            </ModalBody>
            <ModalFooter className="mt-4 d-flex justify-content-between w-100">
                <div className="d-flex gap-2">
                    <Button
                        onClick={navigateToPreviousTerm}
                        disabled={!canNavigateToPreviousTerm}
                        className="rounded-pill"
                    >
                        Previous
                    </Button>
                    <Button onClick={navigateToNextTerm} disabled={!canNavigateToNextTerm} className="rounded-pill">
                        Next
                    </Button>
                </div>
                <Button className="rounded-pill" color="primary" onClick={toggleModal} type="button">
                    Close
                </Button>
            </ModalFooter>
        </Modal>
    );
}
