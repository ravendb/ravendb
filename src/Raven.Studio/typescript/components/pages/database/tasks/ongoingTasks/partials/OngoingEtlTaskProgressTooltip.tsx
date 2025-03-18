import React from "react";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import { OngoingTaskNodeEtlProgressDetails } from "components/models/tasks";
import { NamedProgress, NamedProgressItem } from "components/common/NamedProgress";
import { Icon } from "components/common/Icon";
import copyToClipboard from "common/copyToClipboard";
import { NodeInfoFailure } from "components/pages/database/tasks/ongoingTasks/partials/NodeInfoFailure";
import { loadStatus } from "components/models/common";
import Button from "react-bootstrap/Button";
import Modal from "components/common/Modal";

interface OngoingTaskEtlProgressTooltipProps {
    target: HTMLElement;
    status: loadStatus;
    hasError: boolean;
    progress: OngoingTaskNodeEtlProgressDetails[];
    showPreview: (transformationName: string) => void;
    toggleErrorModal: () => void;
}

export function OngoingEtlTaskProgressTooltip(props: OngoingTaskEtlProgressTooltipProps) {
    const { target, showPreview, toggleErrorModal, status, hasError, progress } = props;

    if (status === "failure") {
        return <NodeInfoFailure target={target} openErrorModal={toggleErrorModal} />;
    }

    if (status !== "success") {
        return null;
    }

    const hasAnyDetailsToShow = progress?.length > 0 || hasError;

    if (!hasAnyDetailsToShow) {
        return null;
    }

    return (
        <PopoverWithHover rounded="true" target={target} placement="top">
            <div className="vstack gap-3 py-2">
                {hasError && (
                    <div className="text-center">
                        <Button variant="danger" key="button" onClick={toggleErrorModal} className="rounded-pill">
                            Open error in modal <Icon icon="newtab" margin="ms-1" />
                        </Button>
                    </div>
                )}
                {progress &&
                    progress.map((transformationScriptProgress, index) => {
                        const nameNode = (
                            <div className="d-flex align-items-center justify-content-center gap-1">
                                {transformationScriptProgress.transformationName}
                                <Button
                                    variant="link"
                                    className="p-0"
                                    size="xs"
                                    title="Show script preview"
                                    onClick={() => showPreview(transformationScriptProgress.transformationName)}
                                >
                                    <Icon icon="preview" margin="m-0" />
                                </Button>
                            </div>
                        );

                        return (
                            <div key={transformationScriptProgress.transformationName} className="vstack">
                                {transformationScriptProgress.transactionalId && (
                                    <div className="vstack">
                                        <div className="small-label d-flex align-items-center justify-content-center gap-1">
                                            Transactional Id
                                            <Button
                                                variant="link"
                                                className="p-0"
                                                size="xs"
                                                onClick={() =>
                                                    copyToClipboard.copy(
                                                        transformationScriptProgress.transactionalId,
                                                        "Transactional Id was copied to clipboard."
                                                    )
                                                }
                                                title="Copy to clipboard"
                                            >
                                                <Icon icon="copy" margin="0" />
                                            </Button>
                                        </div>
                                        <small className="text-center mb-1 px-3">
                                            {transformationScriptProgress.transactionalId}
                                        </small>
                                    </div>
                                )}
                                <NamedProgress name={nameNode}>
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
                                {index !== progress.length - 1 && <hr className="mt-2 mb-0" />}
                            </div>
                        );
                    })}
            </div>
        </PopoverWithHover>
    );
}
