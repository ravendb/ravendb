import { Getter } from "@tanstack/react-table";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import document from "models/database/documents/document";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";

interface CellDocumentPreviewProps {
    document: document;
}

export default function CellDocumentPreview({ document }: CellDocumentPreviewProps) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    const jsonBody = JSON.stringify(document.toDto(true), null, 4);

    const shardNumber = document.__metadata?.shardNumber;
    const shardText = shardNumber != null ? " (shard #" + shardNumber + ")" : "";

    return (
        <>
            <Button type="button" title="Show preview" variant="link" onClick={toggleIsOpen}>
                <Icon icon="preview" margin="m-0" />
            </Button>
            <Modal onHide={toggleIsOpen} show={isOpen} size="lg">
                <Modal.Header onCloseClick={toggleIsOpen} className="pb-0">
                    <div className="d-flex justify-content-between">
                        <div>
                            <Icon icon="document" />
                            {document.getId() ? (
                                <span>
                                    Document:{" "}
                                    <strong>
                                        {document.getId()} {shardText}
                                    </strong>
                                </span>
                            ) : (
                                <span>Document Preview</span>
                            )}
                        </div>
                    </div>
                </Modal.Header>
                <Modal.Body className="pb-3">
                    <pre style={{ maxHeight: "400px" }} className="overflow-auto m-0 mt-3">
                        <Code language="json" code={jsonBody} elementToCopy={jsonBody} />
                    </pre>
                </Modal.Body>
                <Modal.Footer>
                    <Button type="button" variant="secondary" onClick={toggleIsOpen}>
                        <Icon icon="close" />
                        Close
                    </Button>
                </Modal.Footer>
            </Modal>
        </>
    );
}

export function CellDocumentPreviewWrapper({ getValue }: { getValue: Getter<document> }) {
    return <CellDocumentPreview document={getValue()} />;
}
