import React, { useState } from "react";
import { Alert, Button, Modal, ModalBody, ModalFooter } from "reactstrap";
import { Icon } from "components/common/Icon";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import { IndexSharedInfo } from "components/models/indexes";
import IndexUtils from "components/utils/IndexUtils";

interface ConfirmResetIndexesProps {
    indexes: IndexSharedInfo[];
    allActionContexts: DatabaseActionContexts[];
    mode: Raven.Client.Documents.Indexes.IndexResetMode;
    closeConfirm: () => void;
    onConfirm: (indexNames: string[], contexts: DatabaseActionContexts[]) => void;
}

export function ConfirmResetIndexes(props: ConfirmResetIndexesProps) {
    const { indexes, mode, allActionContexts, onConfirm, closeConfirm } = props;

    const hasAutoIndexes = indexes.some(IndexUtils.isAutoIndex);
    const hasReplacements = indexes.some(IndexUtils.isSideBySide);

    const getIndexNamesToReset = (): string[] => {
        let indexesToReset = [...indexes];

        if (hasAutoIndexes && mode === "SideBySide") {
            indexesToReset = indexesToReset.filter((x) => !IndexUtils.isAutoIndex(x));
        }
        if (hasReplacements && mode === "SideBySide") {
            indexesToReset = indexesToReset.filter((x) => !IndexUtils.isSideBySide(x));
        }

        return indexesToReset.map((x) => x.name);
    };

    const getSideBySideWarning = (): string => {
        if (mode !== "SideBySide") {
            return null;
        }

        let prefix = "";

        if (hasAutoIndexes && !hasReplacements) {
            prefix = "Auto indexes";
        }
        if (hasReplacements && !hasAutoIndexes) {
            prefix = "Replacements";
        }
        if (hasAutoIndexes && hasReplacements) {
            prefix = "Auto indexes and replacements";
        }
        if (prefix === "") {
            return null;
        }

        return prefix + " cannot be reset Side by side so they will be skipped.";
    };

    const sideBySideWarning = getSideBySideWarning();
    const indexNamesToReset = getIndexNamesToReset();

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const onSubmit = () => {
        onConfirm(indexNamesToReset, selectedActionContexts);
        closeConfirm();
    };

    return (
        <Modal isOpen toggle={closeConfirm} wrapClassName="bs5" centered contentClassName="modal-border bulge-warning">
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="index" color="warning" addon="reset-index" className="fs-1" margin="m-0" />
                </div>
                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={closeConfirm} />
                </div>
                <div className="text-center lead">
                    You&apos;re about to <span className="text-warning">reset</span> following{" "}
                    {indexNamesToReset.length === 1 ? "index" : `indexes`}
                </div>
                <ul className="overflow-auto" style={{ maxHeight: "200px" }}>
                    {indexNamesToReset.map((indexName) => (
                        <li key={indexName}>{indexName}</li>
                    ))}
                </ul>
                {sideBySideWarning && (
                    <Alert color="warning" className="d-flex align-items-center">
                        <Icon icon="warning" color="warning" />
                        {sideBySideWarning}
                    </Alert>
                )}
                <Alert color="warning" className="d-flex align-items-center">
                    <Icon icon="warning" color="warning" />
                    <div>
                        <strong>Reset</strong> will remove all existing indexed data
                        {ActionContextUtils.showContextSelector(allActionContexts) ? (
                            <span> from the selected context.</span>
                        ) : (
                            <span> from node {allActionContexts[0].nodeTag}.</span>
                        )}
                        <br />
                        All items matched by the index definition will be re-indexed.
                    </div>
                </Alert>
                <Alert color="info">
                    <Icon icon="info" color="info" />
                    <strong>Reset mode: </strong>
                    {mode === "InPlace" && <span>In place</span>}
                    {mode === "SideBySide" && <span>Side by side</span>}
                </Alert>
                {ActionContextUtils.showContextSelector(allActionContexts) && (
                    <div>
                        <h4>Select context</h4>
                        <MultipleDatabaseLocationSelector
                            allActionContexts={allActionContexts}
                            selectedActionContexts={selectedActionContexts}
                            setSelectedActionContexts={setSelectedActionContexts}
                        />
                    </div>
                )}
            </ModalBody>
            <ModalFooter>
                <Button color="link" onClick={closeConfirm} className="link-muted">
                    Cancel
                </Button>
                <Button
                    color="warning"
                    onClick={onSubmit}
                    className="rounded-pill"
                    disabled={selectedActionContexts.length === 0}
                >
                    <Icon icon="reset-index" />
                    Reset
                </Button>
            </ModalFooter>
        </Modal>
    );
}
