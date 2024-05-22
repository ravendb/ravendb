import React, { useState } from "react";
import { Alert, Button, Modal, ModalBody, ModalFooter } from "reactstrap";
import { Icon } from "components/common/Icon";
import { DragAndDrop } from "components/common/DragAndDrop";
import { todo } from "common/developmentHelper";

interface ImportIndexesProps {
    toggle: () => void;
}

todo("Feature", "Damian", "Add logic for Import indexes");

export function ImportIndexes(props: ImportIndexesProps) {
    const { toggle } = props;
    const [fileValid, setFileValid] = useState(false); // State to track if the file is valid

    const handleDrop = (acceptedFiles: File[]) => {
        if (acceptedFiles.length > 0) {
            const file = acceptedFiles[0];
            if (file.name.endsWith(".ravendbdump")) {
                setFileValid(true);
            } else {
                setFileValid(false);
            }
        } else {
            setFileValid(false);
        }
    };

    return (
        <Modal
            isOpen
            toggle={toggle}
            size="lg"
            wrapClassName="bs5"
            contentClassName={`modal-border bulge-primary`}
            centered
        >
            <ModalBody className="vstack gap-4 position-relative">
                <Icon icon="index-import" color="primary" className="text-center fs-1" margin="m-0" />
                <div className="lead text-center">
                    You&apos;re about to <span className="fw-bold">import</span> indexes
                </div>
                <DragAndDrop onDrop={handleDrop} maxFiles={1} validExtension=".ravendbdump" fileValid={fileValid} />
                <Alert color="info" className="text-left">
                    <Icon icon="info" />
                    All the conflicting indexes will be overwritten after the import is done
                </Alert>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={toggle} />
                </div>
            </ModalBody>
            <ModalFooter>
                <Button type="button" color="link" className="link-muted" onClick={toggle}>
                    Cancel
                </Button>
                <Button
                    type="submit"
                    color="success"
                    disabled={!fileValid}
                    title="Import indexes"
                    className="rounded-pill"
                >
                    <Icon icon="import" />
                    Import indexes
                </Button>
            </ModalFooter>
        </Modal>
    );
}
