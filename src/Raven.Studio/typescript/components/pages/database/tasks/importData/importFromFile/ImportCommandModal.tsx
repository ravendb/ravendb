import React, { useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";
import { Icon } from "components/common/Icon";
import Code from "components/common/Code";
import { ImportFromFileFormData } from "./importFromFileValidation";
import { buildImportCurlCommand, ImportCommandType } from "./importFromFileUtils";
import { useImportRestrictions } from "components/pages/database/tasks/importData/shared/useImportRestrictions";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useStudioTranslation } from "components/hooks/useStudioTranslation";

interface ImportCommandModalProps {
    onClose: () => void;
}

export default function ImportCommandModal({ onClose }: ImportCommandModalProps) {
    const t = useStudioTranslation("importData");
    const { control } = useFormContext<ImportFromFileFormData>();
    const formData = useWatch({ control });
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const [commandType, setCommandType] = useState<ImportCommandType>("PowerShell");

    const { restrictedSettingKeys, restrictedOngoingTaskKeys, restrictedConnectionStringKeys } =
        useImportRestrictions();
    const curlCommand = buildImportCurlCommand(
        commandType,
        formData,
        databaseName,
        restrictedSettingKeys,
        restrictedOngoingTaskKeys,
        restrictedConnectionStringKeys
    );

    return (
        <Modal size="lg" show onHide={onClose} className="modal-border bulge-primary">
            <Modal.Header closeButton onCloseClick={onClose} className="pb-2">
                <h3 className="mb-0">
                    <Icon icon="console" color="primary" /> {t("importFile.commandModal.title")}
                </h3>
            </Modal.Header>
            <Modal.Body className="pt-0">
                <p className="text-muted mb-4">{t("importFile.commandModal.description")}</p>
                <div className="d-flex gap-1 mb-2">
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
                    <Icon icon="close" /> {t("close")}
                </Button>
            </Modal.Footer>
        </Modal>
    );
}

const commandTypes: ImportCommandType[] = ["PowerShell", "Cmd", "Bash"];
