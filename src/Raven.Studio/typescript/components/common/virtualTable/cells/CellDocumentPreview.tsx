import { Getter } from "@tanstack/react-table";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import { Button, CloseButton, Modal, ModalBody, ModalFooter } from "reactstrap";
import document from "models/database/documents/document";

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
            <Button type="button" title="Show preview" color="link" onClick={toggleIsOpen}>
                <Icon icon="preview" margin="m-0" />
            </Button>
            <Modal toggle={toggleIsOpen} isOpen={isOpen} wrapClassName="bs5" size="lg" centered>
                <ModalBody className="pb-3">
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
                        <CloseButton onClick={toggleIsOpen} />
                    </div>
                    <pre style={{ maxHeight: "400px" }} className="overflow-auto m-0 mt-3">
                        <Code language="json" code={jsonBody} elementToCopy={jsonBody} />
                    </pre>
                </ModalBody>
                <ModalFooter>
                    <Button type="button" onClick={toggleIsOpen}>
                        <Icon icon="close" />
                        Close
                    </Button>
                </ModalFooter>
            </Modal>
        </>
    );
}

export function CellDocumentPreviewWrapper({ getValue }: { getValue: Getter<document> }) {
    return <CellDocumentPreview document={getValue()} />;
}
