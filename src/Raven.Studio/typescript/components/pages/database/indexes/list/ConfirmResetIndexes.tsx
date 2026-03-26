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
import classNames from "classnames";
import Accordion from "react-bootstrap/Accordion";
import Badge from "react-bootstrap/Badge";

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
    const defaultActiveKey = ["0"];

    const [selectedActionContexts, setSelectedActionContexts] = useState<DatabaseActionContexts[]>(allActionContexts);

    const onSubmit = () => {
        onConfirm(indexNamesToReset, selectedActionContexts);
        closeConfirm();
    };

    return (
        <Modal show onHide={closeConfirm} contentClassName="modal-border bulge-warning" size="lg">
            <Modal.Header className="hstack pb-2 mb-0" onCloseClick={closeConfirm}>
                <div className="text-center lead">Confirm Reset Operation</div>
            </Modal.Header>
            <Modal.Body className="vstack gap-4 pt-0">
                <div className="vstack gap-2">
                    <Accordion
                        className="bs5 accordion-inside-modal"
                        alwaysOpen
                        flush
                        defaultActiveKey={defaultActiveKey}
                    >
                        <Accordion.Item eventKey="0">
                            <Accordion.Header>
                                Indexes to <strong className="text-warning margin-left-xxxs">Reset</strong>{" "}
                                <Badge className="ms-1 px-1 align-self-center rounded-circle" bg="secondary">
                                    {indexNamesToReset.length}
                                </Badge>
                            </Accordion.Header>
                            <Accordion.Collapse unmountOnExit mountOnEnter eventKey="0">
                                <Accordion.Body className="pb-2 pt-0 overflow-scroll" style={{ maxHeight: "160px" }}>
                                    <div className="vstack gap-1">
                                        {indexNamesToReset.map((indexName) => (
                                            <div key={indexName} className="d-flex">
                                                <div
                                                    className={classNames(
                                                        "bg-secondary rounded-pill px-2 py-1 d-flex me-2 align-self-start"
                                                    )}
                                                >
                                                    <Icon icon="index" margin="m-0" />
                                                </div>
                                                <div className="word-break align-self-center">{indexName}</div>
                                            </div>
                                        ))}
                                    </div>
                                </Accordion.Body>
                            </Accordion.Collapse>
                        </Accordion.Item>
                    </Accordion>
                    <div className="vstack gap-1">
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
                                <span>All items matched by the index definition will be re-indexed.</span>
                            </div>
                        </RichAlert>
                        <RichAlert variant="info">
                            <strong>Reset mode: </strong>
                            {mode === "InPlace" && <span>In place</span>}
                            {mode === "SideBySide" && <span>Side by side</span>}
                        </RichAlert>
                    </div>
                </div>
                {ActionContextUtils.showContextSelector(allActionContexts) && (
                    <div>
                        <h4 className="fw-light mb-1">Select context</h4>
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
