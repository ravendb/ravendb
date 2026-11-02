import { Meta } from "@storybook/react-webpack5";
import Modal from "./Modal";
import React, { useState } from "react";
import useBoolean from "components/hooks/useBoolean";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { HrHeader } from "./HrHeader";
import Button from "react-bootstrap/Button";
import { Icon } from "./Icon";
import { ModalProps } from "react-bootstrap/Modal";
import Select, { SelectOption } from "./select/Select";

const selectOptions: SelectOption[] = [
    { value: "one", label: "One" },
    { value: "two", label: "Two" },
    { value: "three", label: "Three" },
];

export default {
    title: "Bits/Modals",
    component: Modal,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof Modal>;

export function Modals() {
    const [modalSize, setModalSize] = useState<ModalProps["size"]>("sm");
    const { value: basicModalOpen, setTrue: openBasicModal, setFalse: closeBasicModal } = useBoolean(false);
    const { value: loadingModalOpen, setTrue: openLoadingModal, setFalse: closeLoadingModal } = useBoolean(false);
    const { value: styledModalOpen, setTrue: openStyledModal, setFalse: closeStyledModal } = useBoolean(false);
    const { value: scrollingModalOpen, setTrue: openScrollingModal, setFalse: closeScrollingModal } = useBoolean(false);
    const { value: sizesModalOpen, setTrue: openSizesModal, setFalse: closeSizesModal } = useBoolean(false);
    const {
        value: headerVariationsOpen,
        setTrue: openHeaderVariations,
        setFalse: closeHeaderVariations,
    } = useBoolean(false);
    const { value: formModalOpen, setTrue: openFormModal, setFalse: closeFormModal } = useBoolean(false);

    return (
        <div className="vstack gap-4">
            <HrHeader>Basic Modal</HrHeader>
            <div>
                <Button variant="primary" onClick={openBasicModal}>
                    Open Basic Modal
                </Button>
            </div>
            <Modal show={basicModalOpen} onHide={closeBasicModal}>
                <Modal.Header closeButton onHide={closeBasicModal}>
                    Basic Modal
                </Modal.Header>
                <Modal.Body>
                    <p>This is a simple modal with header, body and footer.</p>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={closeBasicModal}>
                        Close
                    </Button>
                    <Button variant="primary">Save Changes</Button>
                </Modal.Footer>
            </Modal>

            <HrHeader>Loading State</HrHeader>
            <div>
                <Button variant="primary" onClick={openLoadingModal}>
                    Open Loading Modal
                </Button>
            </div>
            <Modal show={loadingModalOpen} onHide={closeLoadingModal} isLoading={true}>
                <Modal.Body>
                    <p>This content is hidden while the loading indicator is shown.</p>
                </Modal.Body>
            </Modal>

            <HrHeader>Styled Modal</HrHeader>
            <div>
                <Button variant="primary" onClick={openStyledModal}>
                    Open Styled Modal
                </Button>
            </div>
            <Modal show={styledModalOpen} onHide={closeStyledModal} contentClassName="modal-border bulge-primary">
                <Modal.Body className="vstack gap-4 position-relative">
                    <div className="position-absolute m-2 end-0 top-0">
                        <Button variant="close" onClick={closeStyledModal} />
                    </div>
                    <div className="text-center">
                        <Icon icon="database" color="primary" className="fs-1" margin="m-0" />
                    </div>
                    <div className="text-center lead">Custom Styled Modal</div>
                    <p>This modal uses custom styling with bulge-primary class and custom layout.</p>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="outline-secondary" onClick={closeStyledModal}>
                        Cancel
                    </Button>
                    <Button variant="primary">Confirm</Button>
                </Modal.Footer>
            </Modal>

            <HrHeader>Scrolling Content</HrHeader>
            <div>
                <Button variant="primary" onClick={openScrollingModal}>
                    Open Scrolling Modal
                </Button>
            </div>
            <Modal scrollable show={scrollingModalOpen} onHide={closeScrollingModal}>
                <Modal.Header closeButton onHide={closeScrollingModal}>
                    Modal with Scrolling Content
                </Modal.Header>
                <Modal.Body>
                    {Array.from({ length: 30 }).map((_, i) => (
                        <p key={i}>Content line {i + 1} - This is example text to demonstrate scrolling.</p>
                    ))}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={closeScrollingModal}>
                        Close
                    </Button>
                </Modal.Footer>
            </Modal>

            <HrHeader>Form Wrapper with Select</HrHeader>
            <div>
                <Button variant="primary" onClick={openFormModal}>
                    Open Form Modal
                </Button>
            </div>
            <Modal show={formModalOpen} onHide={closeFormModal}>
                <form onSubmit={(e) => e.preventDefault()}>
                    <Modal.Header closeButton onHide={closeFormModal}>
                        Form Modal with Scrolling Content
                    </Modal.Header>
                    <Modal.Body className="vstack gap-3">
                        {Array.from({ length: 20 }).map((_, i) => (
                            <p key={i} className="mb-0">
                                Content line {i + 1} - The body must scroll and the footer must stay visible.
                            </p>
                        ))}
                        <Select options={selectOptions} placeholder="Select near the footer" />
                    </Modal.Body>
                    <Modal.Footer>
                        <Button variant="secondary" onClick={closeFormModal}>
                            Close
                        </Button>
                        <Button variant="primary" type="submit">
                            Save
                        </Button>
                    </Modal.Footer>
                </form>
            </Modal>

            <HrHeader>Modal Sizes</HrHeader>
            <div>
                <Button variant="primary" onClick={openSizesModal}>
                    Show Modal Sizes
                </Button>
            </div>
            <Modal show={sizesModalOpen} onHide={closeSizesModal} size={modalSize}>
                <Modal.Header closeButton onHide={closeSizesModal}>
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
                        <Button onClick={closeSizesModal} variant="secondary" size="sm">
                            Close
                        </Button>
                    </div>
                </Modal.Body>
            </Modal>

            <HrHeader>Header Variations</HrHeader>
            <div>
                <Button variant="primary" onClick={openHeaderVariations}>
                    Show Header Variations
                </Button>
            </div>
            <Modal show={headerVariationsOpen} onHide={closeHeaderVariations}>
                <Modal.Header closeButton onCloseClick={closeHeaderVariations}>
                    <div className="d-flex align-items-center">
                        <Icon icon="document" color="primary" margin="me-2" />
                        <span>Modal with Icon in Header</span>
                    </div>
                </Modal.Header>
                <Modal.Body>
                    <p>This modal demonstrates a header with an icon.</p>
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={closeHeaderVariations}>
                        Close
                    </Button>
                </Modal.Footer>
            </Modal>
        </div>
    );
}
