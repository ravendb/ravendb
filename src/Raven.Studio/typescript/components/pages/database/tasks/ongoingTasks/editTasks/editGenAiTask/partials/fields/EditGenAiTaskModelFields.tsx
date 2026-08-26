import {
    FormAceEditor,
    FormLabel,
    FormGroup,
    FormErrorIcon,
    FormSwitch,
    FormDurationPicker,
} from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { useRef } from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import AceEditor from "components/common/ace/AceEditor";
import AceEditorSamplesPanel, {
    AceEditorSamplesToggleAction,
    confirmSampleLoad,
} from "components/common/ace/AceEditorSamplesPanel";
import useConfirm from "components/common/ConfirmDialog";
import ReactAce from "react-ace";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import AiAssistantWindow from "components/common/aiAssistant/AiAssistantWindow";
import AiAssistantButton from "components/common/aiAssistant/AiAssistantButton";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";
import Button from "react-bootstrap/Button";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import useEditGenAiTaskToolsSection from "../../hooks/useEditGenAiTaskToolsSection";
import useBoolean from "components/hooks/useBoolean";
import EditGenAiTaskQueryToolItem from "./EditGenAiTaskQueryToolItem";
import { jsonSchemaSamplesTabs, promptSamplesTabs, sampleObjectSamplesTabs } from "../../editGenAiTaskSamplesData";

export default function EditGenAiTaskModelFields() {
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();
    const toolsEditor = useEditGenAiTaskToolsSection();
    const { value: isToolsPanelOpen, setValue: setIsToolsPanelOpen, toggle: toggleIsToolsPanelOpen } = useBoolean(true);

    const formValues = useWatch({ control });

    const promptRef = useRef<ReactAce>(null);

    const { value: isAiAssistOpen, toggle: toggleIsAiAssistOpen } = useBoolean(false);
    const {
        value: isPromptSamplesOpen,
        toggle: togglePromptSamples,
        setValue: setIsPromptSamplesOpen,
    } = useBoolean(false);

    const confirm = useConfirm();

    const handleLoadPromptSample = async (script: string) => {
        if (await confirmSampleLoad(confirm, formValues.prompt ?? "")) {
            setValue("prompt", script, { shouldValidate: true, shouldDirty: true });
        }
    };

    return (
        <>
            <FormGroup>
                <FormLabel>
                    Prompt
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Define the instruction that will be sent to the model.
                                <br />
                                It will be applied to each context object generated in the previous step.
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <div className="position-relative">
                    <FormAceEditor
                        aceRef={promptRef}
                        control={control}
                        name="prompt"
                        mode="text"
                        actions={[
                            { component: <AceEditor.FullScreenAction /> },
                            {
                                component: <AceEditorSamplesToggleAction onClick={togglePromptSamples} />,
                                position: "bottom",
                            },
                        ]}
                        wrapEnabled
                        setOptions={{
                            indentedSoftWrap: false,
                        }}
                        isFullScreenLabelHidden
                    />
                    {formValues.prompt?.length > 0 && (
                        <AiAssistantButton handleClick={toggleIsAiAssistOpen} right="48px" />
                    )}
                    {isAiAssistOpen && (
                        <AiAssistantWindow
                            data={{
                                View: "GenAI",
                                Message: getRefinePromptMessage(formValues),
                            }}
                            acceptResult={(text) => setValue("prompt", text)}
                            successMessage="AI refined your prompt based on your input and information from the previous steps."
                            closeWindow={toggleIsAiAssistOpen}
                            right="48px"
                        />
                    )}
                </div>
                <AceEditorSamplesPanel
                    isOpen={isPromptSamplesOpen}
                    tabs={promptSamplesTabs}
                    onSelect={handleLoadPromptSample}
                    onClose={() => setIsPromptSamplesOpen(false)}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName="sampleObject"
                sampleObject={formValues.sampleObject}
                sampleObjectSamplesPanel={{ tabs: sampleObjectSamplesTabs }}
                jsonSchemaName="jsonSchema"
                jsonSchema={formValues.jsonSchema}
                jsonSchemaSamplesPanel={{ tabs: jsonSchemaSamplesTabs }}
                canRegenerateSchemaName="canRegenerateSchema"
            />
            <div className="hstack mt-3 mb-1">
                <h3 className="mb-0">
                    Define query tools
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Define queries that the LLM can use to retrieve data from the database.
                                <br />
                                <br />
                                These queries are executed in the background by an agent. The LLM does not access the
                                database directly.
                            </>
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </h3>
                <FormErrorIcon control={control} paths={["queries"]} onError={() => setIsToolsPanelOpen(true)} />
                <CollapseButton isExpanded={isToolsPanelOpen} toggle={toggleIsToolsPanelOpen} />
            </div>
            <Collapse in={isToolsPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <div className="panel-bg-1 p-3 rounded-2 border border-secondary mb-2">
                        <div className="hstack justify-content-between">
                            <div className="hstack gap-2">
                                <div className="tool-icon bg-faded-primary border border-primary">
                                    <Icon icon="query" color="primary" margin="m-0" />
                                </div>
                                <div>Query tools</div>
                            </div>
                            <Button variant="primary" className="rounded-pill" onClick={toolsEditor.handleAddQuery}>
                                <Icon icon="plus" />
                                Add new query tool
                            </Button>
                        </div>
                        <div className="vstack">
                            {toolsEditor.queriesFieldArray.fields.map((field, index) => (
                                <EditGenAiTaskQueryToolItem
                                    key={field.id}
                                    index={index}
                                    remove={() => toolsEditor.handleRemoveQuery(index)}
                                    save={() => toolsEditor.handleSaveQuery(index)}
                                    edit={() => toolsEditor.handleEditQuery(index)}
                                />
                            ))}
                        </div>
                    </div>
                </div>
            </Collapse>
            <TracingFields />
        </>
    );
}

function TracingFields() {
    const { control } = useFormContext<EditGenAiTaskFormData>();

    const formValues = useWatch({
        control,
    });

    return (
        <>
            <FormGroup marginClass="mb-0 mt-2">
                <FormSwitch control={control} name="isEnableTracing">
                    Enable conversation documents
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Toggle on to store the internal interaction between the GenAI Task, the LLM, and the
                                agent that runs in the background (including query calls and responses) as a
                                conversation document in the <code>@conversations</code> collection.
                                <br />
                                <br />
                                The document ID will be prefixed with the GenAI Task ID.
                            </>
                        }
                    >
                        <Icon icon="info-new" />
                    </PopoverWithHoverWrapper>
                </FormSwitch>
            </FormGroup>
            {formValues.isEnableTracing && (
                <FormGroup marginClass="mt-1">
                    <FormSwitch control={control} name="isSetTracingExpiration">
                        Set conversation documents expiration
                        <PopoverWithHoverWrapper message="Toggle on to set how long conversation documents are retained.">
                            <Icon icon="info-new" />
                        </PopoverWithHoverWrapper>
                    </FormSwitch>
                    {formValues.isSetTracingExpiration && (
                        <FormDurationPicker control={control} name="tracingExpirationInSeconds" showDays isFlexGrow />
                    )}
                </FormGroup>
            )}
        </>
    );
}

function getRefinePromptMessage(formValues: EditGenAiTaskFormData) {
    return `## Original prompt
${formValues.prompt}

## Source collection name
${formValues.collectionName}

## Context generation script
${formValues.script}

## Output structure
${formValues.jsonSchema || formValues.sampleObject || ""}`;
}
