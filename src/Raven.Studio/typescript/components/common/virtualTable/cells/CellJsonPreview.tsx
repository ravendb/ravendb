import { Getter } from "@tanstack/react-table";
import Code from "components/common/Code";
import { Icon } from "components/common/Icon";
import useBoolean from "components/hooks/useBoolean";
import Button from "react-bootstrap/Button";
import { CloseButton, Modal, ModalBody, ModalFooter } from "reactstrap";
import document from "models/database/documents/document";

interface CellJsonPreviewProps {
    json: object;
}

export default function CellJsonPreview({ json }: CellJsonPreviewProps) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    const jsonText = JSON.stringify(json, null, 4);

    return (
        <>
            <Button type="button" title="Show preview" variant="link" onClick={toggleIsOpen}>
                <Icon icon="preview" margin="m-0" />
            </Button>
            <Modal toggle={toggleIsOpen} isOpen={isOpen} wrapClassName="bs5" size="lg" centered>
                <ModalBody className="pb-3">
                    <div className="d-flex justify-content-between">
                        <div>
                            <Icon icon="json" />
                            <span>Preview</span>
                        </div>
                        <CloseButton onClick={toggleIsOpen} />
                    </div>
                    <pre style={{ maxHeight: "400px" }} className="overflow-auto m-0 mt-3">
                        <Code language="json" code={jsonText} elementToCopy={jsonText} />
                    </pre>
                </ModalBody>
                <ModalFooter>
                    <Button variant="secondary" type="button" onClick={toggleIsOpen}>
                        <Icon icon="close" />
                        Close
                    </Button>
                </ModalFooter>
            </Modal>
        </>
    );
}

export function CellJsonPreviewWrapper({ getValue }: { getValue: Getter<document> }) {
    return <CellJsonPreview json={getValue()} />;
}
