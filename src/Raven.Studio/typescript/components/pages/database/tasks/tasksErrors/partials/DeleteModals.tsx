import React, { useState } from "react";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Modal from "components/common/Modal";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import RichAlert from "components/common/RichAlert";
import { Switch } from "components/common/Checkbox";
import { FormGroup, FormLabel } from "components/common/Form";
import { useServices } from "hooks/useServices";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAsync, useAsyncCallback } from "react-async-hook";
import studioSettings from "common/settings/studioSettings";
import messagePublisher from "common/messagePublisher";
import DatabaseUtils from "components/utils/DatabaseUtils";
import {
    EtlTaskWithErrors,
    EtlTransformationWithErrors,
    getTaskCategory,
    TaskCategory,
} from "../utils/tasksErrorsUtils";
import footer from "common/shell/footer";

function useDeleteConfirmation(isRequireTypedConfirm: boolean) {
    const [confirmText, setConfirmText] = useState("");

    const handleTextChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        setConfirmText(e.target.value.trim());
    };

    return {
        confirmText,
        handleTextChange,
        isConfirmed: isRequireTypedConfirm ? confirmText === "DELETE" : true,
    };
}

type DeleteErrorsModalProps =
    | {
          mode: "task";
          toggle: () => void;
          onRefresh: () => void;
          etlName: string;
          etlType?: StudioEtlType;
          transformations: EtlTransformationWithErrors[];
          errorsCount: number;
      }
    | {
          mode: "all";
          toggle: () => void;
          onRefresh: () => void;
          tasksWithErrors: EtlTaskWithErrors[];
      };

export function DeleteErrorsModal(props: DeleteErrorsModalProps) {
    const { toggle, onRefresh, mode } = props;
    const db = useAppSelector(databaseSelectors.activeDatabase);
    const { tasksService } = useServices();

    const asyncGlobalSettings = useAsync(async () => await studioSettings.default.globalSettings(), []);

    const isRequireTypedConfirm =
        asyncGlobalSettings.result?.isRequireTypedConfirmationToDeleteEtlErrors.getValue() ?? true;

    const { confirmText, handleTextChange, isConfirmed } = useDeleteConfirmation(isRequireTypedConfirm);

    const asyncDeleteErrors = useAsyncCallback(async () => {
        const requestsPerTask: { type: TaskCategory; processNames: string[] }[] = [];

        const addTask = (etlName: string, etlType: StudioEtlType | undefined, transformationNames: string[]) => {
            if (transformationNames.length === 0) {
                return;
            }
            requestsPerTask.push({
                type: getTaskCategory(etlType),
                processNames: transformationNames.map((name) => `${etlName}/${name}`),
            });
        };

        if (mode === "task") {
            addTask(
                props.etlName,
                props.etlType,
                props.transformations.map((t) => t.transformationName)
            );
        } else {
            for (const task of props.tasksWithErrors) {
                addTask(
                    task.etlName,
                    task.etlType,
                    task.transformations.map((t) => t.transformationName)
                );
            }
        }

        try {
            const locations = DatabaseUtils.getLocations(db);
            await Promise.all(
                locations.flatMap((location) =>
                    requestsPerTask.map((req) =>
                        tasksService.deleteEtlErrors(db.name, {
                            type: req.type,
                            name: req.processNames,
                            nodeTag: location.nodeTag,
                            shardNumber: location.shardNumber,
                        })
                    )
                )
            );
            messagePublisher.reportSuccess("ETL errors were deleted.");
            footer.default.refreshStats();
            toggle();
            onRefresh();
        } catch (e) {
            messagePublisher.reportError("Failed to delete ETL errors.", e?.responseText, e?.statusText);
        }
    });

    const toggleIsRequireTypedConfirm = async () => {
        if (!asyncGlobalSettings.result) {
            messagePublisher.reportError("Failed to load studio global settings");
            return;
        }

        asyncGlobalSettings.result.isRequireTypedConfirmationToDeleteEtlErrors.setValue(!isRequireTypedConfirm);
        await asyncGlobalSettings.execute();
    };

    return (
        <Modal show contentClassName="modal-border bulge-danger">
            <Modal.Header closeButton onCloseClick={toggle} className="pb-0">
                <h3>
                    <Icon icon="trash" color="danger" />
                    <span>
                        {mode === "task" ? `Delete all errors for task ${props.etlName}?` : "Delete all errors?"}
                    </span>
                </h3>
            </Modal.Header>
            <Modal.Body className="pt-0">
                {mode === "task" ? (
                    <>
                        <p>
                            You are about to delete all <b>{props.errorsCount} errors</b> from <b>{props.etlName}</b>{" "}
                            task.
                        </p>
                        <RichAlert variant="info" icon="info">
                            While the current task errors will be deleted, a task in an <b>Error state</b> will not be
                            set back to the <b>Normal</b> state.
                        </RichAlert>
                    </>
                ) : (
                    <p>
                        You are about to delete errors from <b>{props.tasksWithErrors.length}</b>{" "}
                        {props.tasksWithErrors.length === 1 ? "task" : "tasks"}.
                    </p>
                )}
                {isRequireTypedConfirm && (
                    <FormGroup className="mt-3">
                        <FormLabel className="fw-bold">Type DELETE to confirm</FormLabel>
                        <Form.Control placeholder="DELETE" value={confirmText} onChange={handleTextChange} />
                    </FormGroup>
                )}
            </Modal.Body>
            <Modal.Footer className="hstack justify-content-between">
                <Switch selected={isRequireTypedConfirm} toggleSelection={toggleIsRequireTypedConfirm} color="primary">
                    Require typed confirmation
                </Switch>
                <div className="hstack gap-2 flex-grow-1 justify-content-end">
                    <Button variant="link" onClick={toggle} className="link-muted">
                        Cancel
                    </Button>
                    <ButtonWithSpinner
                        isSpinning={asyncDeleteErrors.loading}
                        variant="danger"
                        onClick={asyncDeleteErrors.execute}
                        className="rounded-pill"
                        disabled={!isConfirmed || asyncDeleteErrors.loading}
                    >
                        Delete
                    </ButtonWithSpinner>
                </div>
            </Modal.Footer>
        </Modal>
    );
}
