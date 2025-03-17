import { Meta } from "@storybook/react";
import Modal from "./Modal";
import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { HrHeader } from "./HrHeader";
import Button from "react-bootstrap/Button";
import { Icon } from "./Icon";
import { ModalProps } from "react-bootstrap/Modal";

export default {
    title: "Bits/Modals",
    component: Modal,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof Modal>;

export function Modals() {
    const [basicModalOpen, setBasicModalOpen] = useState(false);
    const [loadingModalOpen, setLoadingModalOpen] = useState(false);
    const [styledModalOpen, setStyledModalOpen] = useState(false);
    const [scrollingModalOpen, setScrollingModalOpen] = useState(false);
    const [sizesModalOpen, setSizesModalOpen] = useState(false);
    const [modalSize, setModalSize] = useState<ModalProps["size"]>("sm");
    const [headerVariationsOpen, setHeaderVariationsOpen] = useState(false);

    return (
        <div className="vstack gap-4">
            <HrHeader>Basic Modal</HrHeader>
            <div>
                <Button variant="primary" onClick={() => setBasicModalOpen(true)}>
                    Open Basic Modal
                </Button>
            </div>
            <Modal show={basicModalOpen} onHide={() => setBasicModalOpen(false)}>
                <Modal.Header closeButton onHide={() => setBasicModalOpen(false)}>
                    Basic Modal
                </Modal.Header>
                <Modal.Body>
                    <p>This is a simple modal with header, body and footer.</p>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={() => setBasicModalOpen(false)}>
                        Close
                    </Button>
                    <Button variant="primary">Save Changes</Button>
                </Modal.Footer>
            </Modal>

            <HrHeader>Loading State</HrHeader>
            <div>
                <Button variant="primary" onClick={() => setLoadingModalOpen(true)}>
                    Open Loading Modal
                </Button>
            </div>
            <Modal show={loadingModalOpen} onHide={() => setLoadingModalOpen(false)} isLoading={true}>
                <Modal.Body>
                    <p>This content is hidden while the loading indicator is shown.</p>
                </Modal.Body>
            </Modal>

            <HrHeader>Styled Modal</HrHeader>
            <div>
                <Button variant="primary" onClick={() => setStyledModalOpen(true)}>
                    Open Styled Modal
                </Button>
            </div>
            <Modal
                show={styledModalOpen}
                onHide={() => setStyledModalOpen(false)}
                contentClassName="modal-border bulge-primary"
            >
                <Modal.Body className="vstack gap-4 position-relative">
                    <div className="position-absolute m-2 end-0 top-0">
                        <Button variant="close" onClick={() => setStyledModalOpen(false)} />
                    </div>
                    <div className="text-center">
                        <Icon icon="database" color="primary" className="fs-1" margin="m-0" />
                    </div>
                    <div className="text-center lead">Custom Styled Modal</div>
                    <p>This modal uses custom styling with bulge-primary class and custom layout.</p>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="outline-secondary" onClick={() => setStyledModalOpen(false)}>
                        Cancel
                    </Button>
                    <Button variant="primary">Confirm</Button>
                </Modal.Footer>
            </Modal>

            <HrHeader>Scrolling Content</HrHeader>
            <div>
                <Button variant="primary" onClick={() => setScrollingModalOpen(true)}>
                    Open Scrolling Modal
                </Button>
            </div>
            <Modal scrollable show={scrollingModalOpen} onHide={() => setScrollingModalOpen(false)}>
                <Modal.Header closeButton onHide={() => setScrollingModalOpen(false)}>
                    Modal with Scrolling Content
                </Modal.Header>
                <Modal.Body>
                    {Array.from({ length: 30 }).map((_, i) => (
                        <p key={i}>Content line {i + 1} - This is example text to demonstrate scrolling.</p>
                    ))}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={() => setScrollingModalOpen(false)}>
                        Close
                    </Button>
                </Modal.Footer>
            </Modal>

            <HrHeader>Modal Sizes</HrHeader>
            <div>
                <Button variant="primary" onClick={() => setSizesModalOpen(true)}>
                    Show Modal Sizes
                </Button>
            </div>
            <Modal show={sizesModalOpen} onHide={() => setSizesModalOpen(false)} size={modalSize}>
                <Modal.Header closeButton onHide={() => setSizesModalOpen(false)}>
                    Small Modal
                </Modal.Header>
                <Modal.Body>
                    <p>This is a modal with different sizes.</p>
                    <div className="d-grid gap-2">
                        <Button onClick={() => setModalSize("sm")} variant="info" size="sm">
                            Show Default Size
                        </Button>
                        <Button onClick={() => setModalSize("lg")} variant="warning" size="sm">
                            Show Large Size
                        </Button>
                        <Button onClick={() => setModalSize("xl")} variant="danger" size="sm">
                            Show XL Size
                        </Button>
                        <Button onClick={() => setSizesModalOpen(false)} variant="secondary" size="sm">
                            Close
                        </Button>
                    </div>
                </Modal.Body>
            </Modal>

            <HrHeader>Header Variations</HrHeader>
            <div>
                <Button variant="primary" onClick={() => setHeaderVariationsOpen(true)}>
                    Show Header Variations
                </Button>
            </div>
            <Modal show={headerVariationsOpen} onHide={() => setHeaderVariationsOpen(false)}>
                <Modal.Header closeButton onCloseClick={() => setHeaderVariationsOpen(false)}>
                    <div className="d-flex align-items-center">
                        <Icon icon="document" color="primary" margin="me-2" />
                        <span>Modal with Icon in Header</span>
                    </div>
                </Modal.Header>
                <Modal.Body>
                    <p>This modal demonstrates a header with an icon.</p>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={() => setHeaderVariationsOpen(false)}>
                        Close
                    </Button>
                </Modal.Footer>
            </Modal>
        </div>
    );
}
