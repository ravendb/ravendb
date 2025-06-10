import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { useAppDispatch, useAppSelector } from "components/store";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { AboutViewHeading } from "components/common/AboutView";
import useBoolean from "components/hooks/useBoolean";
import AceEditor from "components/common/ace/AceEditor";
import { AceEditorMode } from "components/models/aceEditor";
import Collapse from "react-bootstrap/Collapse";
import { ThemeColor } from "components/models/common";
import EditGenAiTaskInfoHub from "../../EditGenAiTaskInfoHub";
import EditGenAiTaskCancelButton from "../EditGenAiTaskCancelButton";
import { FormSwitch, FormGroup } from "components/common/Form";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { editGenAiTaskUtils } from "../../utils/editGenAiTaskUtils";

export function EditGenAiTaskStepSummary() {
    const dispatch = useAppDispatch();

    const isEditTask = useAppSelector(editGenAiTaskSelectors.isEditTask);

    const { control } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch({ control });

    return (
        <>
            <div className="hstack justify-content-between">
                <AboutViewHeading title="Review task configuration" marginBottom={2} icon="ai-etl" />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                This step summarizes your task configuration.
                <br />
                Please review all settings before saving the task.
            </p>
            <div className="hstack justify-content-between">
                <div>Basic settings</div>
                <Button variant="link" onClick={() => dispatch(editGenAiTaskActions.currentStepSet("basic"))} size="sm">
                    <Icon icon="edit" /> Edit
                </Button>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <div className="hstack justify-content-between">
                    <div>Task name</div>
                    <div>{formValues.name}</div>
                </div>
                <div className="hstack justify-content-between">
                    <div>Task state</div>
                    <div className={getBooleanColorClass(formValues.state === "Enabled")}>{formValues.state}</div>
                </div>
                {formValues.responsibleNode && (
                    <div className="hstack justify-content-between">
                        <div>Responsible node</div>
                        <div>
                            <Icon icon="node" color="node" />
                            {formValues.responsibleNode}
                        </div>
                    </div>
                )}
                <div className="hstack justify-content-between">
                    <div>Connection string</div>
                    <div>{formValues.connectionStringName}</div>
                </div>
                <div className="hstack justify-content-between">
                    <div>Max concurrency</div>
                    <div>{formValues.maxConcurrency || editGenAiTaskUtils.defaultMaxConcurrency}</div>
                </div>
            </div>
            <div className="hstack justify-content-between mt-4">
                <div>Context input</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("context"))}
                    size="sm"
                >
                    <Icon icon="edit" /> Edit
                </Button>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <div className="hstack justify-content-between">
                    <div>Source collection</div>
                    <div>{formValues.collectionName}</div>
                </div>
                <RowWithPreview label="Context generation script" value={formValues.script} mode="javascript" />
            </div>
            <div className="hstack justify-content-between mt-4">
                <div>Prompt & schema</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("modelInput"))}
                    size="sm"
                >
                    <Icon icon="edit" /> Edit
                </Button>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <div className="hstack justify-content-between">
                    <div>Prompt</div>
                    <div style={{ maxWidth: 200 }} className="text-truncate" title={formValues.prompt}>
                        {formValues.prompt}
                    </div>
                </div>
                {formValues.jsonSchema && (
                    <RowWithPreview label="JSON schema" value={formValues.jsonSchema} mode="json" />
                )}
                {formValues.sampleObject && (
                    <RowWithPreview label="Sample object" value={formValues.sampleObject} mode="json" />
                )}
            </div>
            <div className="hstack justify-content-between mt-4">
                <div>Document update</div>
                <Button
                    variant="link"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("updateScript"))}
                    size="sm"
                >
                    <Icon icon="edit" /> Edit
                </Button>
            </div>
            <div className="panel-bg-1 p-3 rounded-2 mt-1">
                <RowWithPreview label="Update script" value={formValues.updateScript} mode="javascript" />
            </div>
            {isEditTask && (
                <div className="hstack justify-content-end mt-4">
                    <FormGroup>
                        <FormSwitch control={control} name="isResetScript">
                            Reprocess all documents
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        If enabled, the task will attempt to reprocess all documents from the source
                                        collection.
                                        <br />
                                        <br />
                                        Note: Documents will not be reprocessed if the hash in their metadata exactly
                                        matches the hash generated from the current task configuration.
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </FormSwitch>
                    </FormGroup>
                </div>
            )}
        </>
    );
}

export function EditGenAiTaskStepSummaryFooter() {
    const dispatch = useAppDispatch();

    return (
        <div className="hstack justify-content-between">
            <div className="hstack gap-2">
                <EditGenAiTaskCancelButton />
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("updateScript"))}
                >
                    <Icon icon="arrow-left" /> Back
                </Button>
            </div>

            <Button type="submit" variant="primary" className="rounded-pill">
                Save <Icon icon="save" margin="ms-1" />
            </Button>
        </div>
    );
}

function RowWithPreview(props: { label: string; value: string; mode: AceEditorMode }) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);

    return (
        <div>
            <div className="hstack justify-content-between">
                <div>{props.label}</div>
                <Button variant="link" size="xs" onClick={toggleIsOpen} className="pe-0">
                    <Icon icon={isOpen ? "collapse-vertical" : "expand-vertical"} />
                    {isOpen ? "Collapse" : "Show"}
                </Button>
            </div>
            <Collapse in={isOpen} mountOnEnter unmountOnExit>
                <div className="mt-2">
                    <AceEditor mode={props.mode} value={props.value || ""} readOnly />
                </div>
            </Collapse>
        </div>
    );
}

function getBooleanColorClass(value: boolean): `text-${ThemeColor}` {
    return value ? "text-success" : "text-danger";
}
