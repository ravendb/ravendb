import { Button, Col, Modal, ModalBody, ModalFooter, Row } from "reactstrap";
import { Icon } from "components/common/Icon";
import Code from "components/common/Code";
import { Row as ReactTableRow } from "@tanstack/react-table";
import { useState } from "react";

interface IndexErrorsModalProps {
    toggleModal: () => void;
    isOpen: boolean;
    dataLength: number;
    errorDetails: ReactTableRow<IndexErrorPerDocument>;
    getRow: (id: string, searchAll?: boolean) => ReactTableRow<IndexErrorPerDocument>;
}

export default function IndexErrorsModal({
    toggleModal,
    dataLength,
    isOpen,
    errorDetails,
    getRow,
}: IndexErrorsModalProps) {
    const [currentErrorDetails, setCurrentErrorDetails] = useState<ReactTableRow<IndexErrorPerDocument>>(errorDetails);

    const canNavigateToPreviousError = currentErrorDetails.index > 0;
    const canNavigateToNextError = currentErrorDetails.index < dataLength - 1;

    const previousError = () => {
        const rowId = String(currentErrorDetails.index - 1);
        setCurrentErrorDetails(getRow(rowId));
    };

    const nextError = () => {
        const rowId = String(currentErrorDetails.index + 1);
        setCurrentErrorDetails(getRow(rowId));
    };

    const errorDetailsOriginal = currentErrorDetails.original;
    return (
        <Modal centered contentClassName="modal-border bulge-warning" wrapClassName="bs5" size="lg" isOpen={isOpen}>
            <ModalBody className="pb-0 vstack modal-details gap-3">
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggleModal} />
                </div>
                <div className="text-center">
                    <Icon icon="warning" color="warning" margin="me-0" className="fs-1" />
                </div>
                <div className="text-center lead">Indexing error details</div>
                <Row className="details-item" title={errorDetailsOriginal.IndexName}>
                    <Col lg={2}>Index name</Col>
                    <Col lg={10} className="text-right fw-bold text-truncate">
                        {errorDetailsOriginal.IndexName}
                    </Col>
                </Row>
                <Row className="details-item" title={errorDetailsOriginal.Document}>
                    <Col lg={2}>Document ID</Col>
                    <Col lg={10} className="text-right fw-bold text-truncate">
                        {errorDetailsOriginal.Document}
                    </Col>
                </Row>
                <Row className="details-item" title={errorDetailsOriginal.LocalTime}>
                    <Col lg={2}>Date</Col>
                    <Col lg={10} className="text-right fw-bold text-truncate">
                        {errorDetailsOriginal.LocalTime}
                    </Col>
                </Row>
                <Row className="details-item" title={errorDetailsOriginal.Action}>
                    <Col lg={2}>Action</Col>
                    <Col lg={10} className="text-right fw-bold text-truncate">
                        {errorDetailsOriginal.Action}
                    </Col>
                </Row>
                <Code code={errorDetailsOriginal.Error} elementToCopy={errorDetailsOriginal.Error} language="csharp" />
            </ModalBody>
            <ModalFooter className="d-flex justify-content-between mt-4">
                <div className="d-flex gap-2">
                    <Button disabled={!canNavigateToPreviousError} onClick={previousError} className="rounded-pill">
                        <Icon icon="arrow-left" />
                        Previous
                    </Button>
                    <Button disabled={!canNavigateToNextError} onClick={nextError} className="rounded-pill">
                        Next
                        <Icon icon="arrow-right" margin="ms-1" />
                    </Button>
                </div>
                <Button className="rounded-pill" color="primary" onClick={toggleModal} type="button">
                    Close
                </Button>
            </ModalFooter>
        </Modal>
    );
}
