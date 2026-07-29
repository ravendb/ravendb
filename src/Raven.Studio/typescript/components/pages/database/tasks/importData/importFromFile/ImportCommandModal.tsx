import React, { useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import ButtonGroup from "react-bootstrap/ButtonGroup";
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
                <h4 className="mb-0">
                    <Icon icon="code" /> Import command
                </h4>
            </Modal.Header>
            <Modal.Body>
                <p className="text-muted">Select your shell and copy the command to import the database dump.</p>
                <ButtonGroup size="sm" className="mb-3">
                    {commandTypes.map((type) => (
                        <Button
                            key={type}
                            variant="secondary"
                            active={commandType === type}
                            onClick={() => setCommandType(type)}
                        >
                            {type}
                        </Button>
                    ))}
                </ButtonGroup>
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
