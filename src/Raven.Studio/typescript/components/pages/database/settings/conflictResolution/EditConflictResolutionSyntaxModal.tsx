import { Button, Modal, ModalBody } from "reactstrap";
import React from "react";
import Code from "components/common/Code";

interface EditConflictResolutionSyntaxModalProps {
    toggle: () => void;
    isOpen: boolean;
}

export function EditConflictResolutionSyntaxModal(props: EditConflictResolutionSyntaxModalProps) {
    const { toggle, isOpen } = props;
    return (
        <Modal
            size="lg"
            wrapClassName="bs5"
            isOpen={isOpen}
            toggle={toggle}
            contentClassName="modal-border bulge-primary"
        >
            <ModalBody>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggle} />
                </div>
                <h5 className="mb-1">Conflicted documents</h5>
                <div className="d-flex gap-3 flex-wrap mb-3">
                    <Code code={conflictedDocument1} language="json" />
                    <Code code={conflictedDocument2} language="json" />
                </div>
                <h5 className="mb-1">Script</h5>
                <Code code={script} language="javascript" hasCopyToClipboard elementToCopy={script} className="mb-3" />
                <h5 className="mb-1">Conflict resolution result</h5>
                <Code code={result} language="json" />
            </ModalBody>
        </Modal>
    );
}

const conflictedDocument1 = `{
    "Name": "John",
    "MaxRecord": 43
}`;

const conflictedDocument2 = `{
    "Name": "John",
    "MaxRecord": 80
}`;

const script = `// The following variables are available in script context:
// docs - array of conflicted documents
// hasTombstone - true if any of conflicted document is deletion
// resolveToTombstone - return this value if you want to resolve conflict by deleting document 

var maxRecord = 0;
for (var i = 0; i < docs.length; i++) {
    maxRecord = Math.max(docs[i].MaxRecord, maxRecord);   
}
docs[0].MaxRecord = maxRecord;

return docs[0];`;

const result = `{
    "Name": "John",
    "MaxRecord": 80
}`;
