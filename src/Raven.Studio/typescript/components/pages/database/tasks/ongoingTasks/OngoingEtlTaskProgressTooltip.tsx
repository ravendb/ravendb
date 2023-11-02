import React from "react";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { OngoingEtlTaskNodeInfo, OngoingTaskInfo } from "components/models/tasks";
import { NamedProgress, NamedProgressItem } from "components/common/NamedProgress";
import { Icon } from "components/common/Icon";
import { Button, Modal, ModalBody } from "reactstrap";
import useBoolean from "components/hooks/useBoolean";
import Code from "components/common/Code";
import copyToClipboard from "common/copyToClipboard";
import { todo } from "common/developmentHelper";

interface OngoingTaskEtlProgressTooltipProps {
    target: HTMLElement;
    nodeInfo: OngoingEtlTaskNodeInfo;
    task: OngoingTaskInfo;
    showPreview: (transformationName: string) => void;
}

export function OngoingEtlTaskProgressTooltip(props: OngoingTaskEtlProgressTooltipProps) {
    const { target, nodeInfo, showPreview } = props;
    const { value: isErrorModalOpen, toggle: toggleErrorModal } = useBoolean(false);

    if (nodeInfo.status === "failure") {
        return (
            <>
                {!isErrorModalOpen && (
                    <PopoverWithHover target={target} placement="top">
                        <div className="vstack gap-2 p-3">
                            <div className="text-danger">
                                <Icon icon="warning" color="danger" /> Unable to load task status:
                            </div>
                            <div
                                className="overflow-auto"
                                style={{
                                    maxWidth: "300px",
                                    maxHeight: "300px",
                                }}
                            >
                                <code>{nodeInfo.details.error}</code>
                            </div>
                            <Button size="sm" onClick={toggleErrorModal}>
                                Open error in modal <Icon icon="newtab" margin="ms-1" />
                            </Button>
                        </div>
                    </PopoverWithHover>
                )}
                <Modal
                    size="xl"
                    wrapClassName="bs5"
                    isOpen={isErrorModalOpen}
                    toggle={toggleErrorModal}
                    contentClassName="modal-border bulge-danger"
                >
                    <ModalBody>
                        <div className="position-absolute m-2 end-0 top-0">
                            <Button close onClick={toggleErrorModal} />
                        </div>
                        <div className="hstack gap-3 mb-4">
                            <div className="text-center">
                                <Icon icon="warning" color="danger" className="fs-1" margin="m-0" />
                            </div>
                            <div className="text-center lead">Unable to load task status:</div>
                        </div>
                        <Code code={nodeInfo.details.error} language="csharp" />

                        <div className="text-end">
                            <Button
                                className="rounded-pill"
                                color="primary"
                                size="xs"
                                onClick={() =>
                                    copyToClipboard.copy(nodeInfo.details.error, "Copied error message to clipboard")
                                }
                            >
                                <Icon icon="copy" /> <span>Copy to clipboard</span>
                            </Button>
                        </div>
                    </ModalBody>
                </Modal>
            </>
        );
    }

    if (nodeInfo.status !== "success") {
        return null;
    }

    todo("Styling", "Kwiato", "Transactional Id position");

    return (
        <PopoverWithHover rounded="true" target={target} placement="top">
            <div className="ongoing-tasks-details-tooltip">
                {nodeInfo.etlProgress &&
                    nodeInfo.etlProgress.map((transformationScriptProgress) => {
                        const nameNode = (
                            <div>
                                {transformationScriptProgress.transformationName}
                                <button
                                    type="button"
                                    className="btn btn-link btn-sm margin-left-xs"
                                    title="Show script preview"
                                    onClick={() => showPreview(transformationScriptProgress.transformationName)}
                                >
                                    <Icon icon="preview" margin="m-0" />
                                </button>
                            </div>
                        );

                        return (
                            <div>
                                {transformationScriptProgress.transactionalId && (
                                    <div className="d-flex justify-content-between p-3">
                                        <div className="small-label">Transactional Id:</div>
                                        <div>
                                            <div>{transformationScriptProgress.transactionalId}</div>
                                            <Button
                                                color="primary"
                                                className="btn-link"
                                                size="xs"
                                                onClick={() =>
                                                    copyToClipboard.copy(
                                                        transformationScriptProgress.transactionalId,
                                                        "Transactional Id was copied to clipboard."
                                                    )
                                                }
                                            >
                                                <Icon icon="copy" margin="0" />
                                            </Button>
                                        </div>
                                    </div>
                                )}
                                <NamedProgress name={nameNode} key={transformationScriptProgress.transformationName}>
                                    <NamedProgressItem progress={transformationScriptProgress.documents}>
                                        documents
                                    </NamedProgressItem>
                                    <NamedProgressItem progress={transformationScriptProgress.documentTombstones}>
                                        tombstones
                                    </NamedProgressItem>
                                    {transformationScriptProgress.counterGroups.total > 0 && (
                                        <NamedProgressItem progress={transformationScriptProgress.counterGroups}>
                                            counters
                                        </NamedProgressItem>
                                    )}
                                </NamedProgress>
                            </div>
                        );
                    })}
            </div>
        </PopoverWithHover>
    );
}
