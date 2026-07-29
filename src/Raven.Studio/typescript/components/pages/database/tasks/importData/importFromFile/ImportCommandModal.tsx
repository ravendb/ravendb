import React, { useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Code from "components/common/Code";
import { ImportFromFileFormData } from "./importFromFileValidation";
import { buildImportCurlCommand, ImportCommandType } from "./importFromFileUtils";
import { useImportLicenseRestrictions } from "./useImportLicenseRestrictions";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

interface ImportCommandModalProps {
    onClose: () => void;
}

export default function ImportCommandModal({ onClose }: ImportCommandModalProps) {
    const { control } = useFormContext<ImportFromFileFormData>();
    const formData = useWatch({ control });
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const [commandType, setCommandType] = useState<ImportCommandType>("PowerShell");

    const { restrictedFeatures, restrictedOngoingTasks } = useImportLicenseRestrictions();
    const curlCommand = buildImportCurlCommand(
        commandType,
        formData as ImportFromFileFormData,
        databaseName,
        restrictedFeatures.map((x) => x.settingKey),
        restrictedOngoingTasks.map((x) => x.taskKey)
    );

    return (
        <Modal size="lg" show onHide={onClose} className="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={onClose}>
                <h3 className="mb-0">
                    <Icon icon="console" color="primary" /> Import command
                </h3>
            </Modal.Header>
            <Modal.Body>
                <p className="text-muted">Select your shell and copy the command to import the database dump.</p>
                <div className="d-flex gap-2 mb-3">
                    {commandTypes.map((type) => (
                        <Button
                            key={type}
                            size="sm"
                            variant={commandType === type ? "secondary" : "outline-secondary"}
                            onClick={() => setCommandType(type)}
                        >
                            {type}
                        </Button>
                    ))}
                </div>
                <Code code={curlCommand} language="plaintext" wrappable isTitleHidden />
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={onClose}>
                    <Icon icon="close" /> Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

const commandTypes: ImportCommandType[] = ["PowerShell", "Cmd", "Bash"];
