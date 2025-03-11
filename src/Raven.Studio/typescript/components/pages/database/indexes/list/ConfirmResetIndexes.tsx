import React, { useState } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import {
    DatabaseActionContexts,
    MultipleDatabaseLocationSelector,
} from "components/common/MultipleDatabaseLocationSelector";
import ActionContextUtils from "components/utils/actionContextUtils";
import { IndexSharedInfo } from "components/models/indexes";
import IndexUtils from "components/utils/IndexUtils";
import RichAlert from "components/common/RichAlert";
import Modal from "components/common/Modal";

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
        <Modal show onHide={closeConfirm} contentClassName="modal-border bulge-warning">
            <Modal.Header className="vstack gap-4" onCloseClick={closeConfirm}>
                <Icon icon="index" color="warning" addon="reset-index" className="fs-1" margin="m-0" />
                <div className="lead">
                    You&apos;re about to <span className="text-warning">reset</span> following{" "}
                    {indexNamesToReset.length === 1 ? "index" : `indexes`}
                </div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4">
                <ul className="overflow-auto" style={{ maxHeight: "200px" }}>
                    {indexNamesToReset.map((indexName) => (
                        <li key={indexName}>{indexName}</li>
                    ))}
                </ul>
                {sideBySideWarning && <RichAlert variant="warning">{sideBySideWarning}</RichAlert>}
                <RichAlert variant="warning">
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
                </RichAlert>
                <RichAlert variant="info">
                    <strong>Reset mode: </strong>
                    {mode === "InPlace" && <span>In place</span>}
                    {mode === "SideBySide" && <span>Side by side</span>}
                </RichAlert>
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
            </Modal.Body>
            <Modal.Footer>
                <Button variant="link" onClick={closeConfirm} className="link-muted">
                    Cancel
                </Button>
                <Button
                    variant="warning"
                    onClick={onSubmit}
                    className="rounded-pill"
                    disabled={selectedActionContexts.length === 0}
                >
                    <Icon icon="reset-index" />
                    Reset
                </Button>
            </Modal.Footer>
        </Modal>
    );
}
