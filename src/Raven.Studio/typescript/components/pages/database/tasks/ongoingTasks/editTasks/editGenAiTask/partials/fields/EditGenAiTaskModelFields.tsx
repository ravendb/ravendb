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
import ReactAce from "react-ace";
import Code from "components/common/Code";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import CollapseButton from "components/common/CollapseButton";
import Collapse from "react-bootstrap/Collapse";
import Button from "react-bootstrap/Button";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import useEditGenAiTaskToolsSection from "../../hooks/useEditGenAiTaskToolsSection";
import useBoolean from "components/hooks/useBoolean";
import EditGenAiTaskQueryToolItem from "./EditGenAiTaskQueryToolItem";

export default function EditGenAiTaskModelFields() {
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();
    const toolsEditor = useEditGenAiTaskToolsSection();
    const { value: isToolsPanelOpen, setValue: setIsToolsPanelOpen, toggle: toggleIsToolsPanelOpen } = useBoolean(true);

    const formValues = useWatch({ control });

    const promptRef = useRef<ReactAce>(null);

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
                <FormAceEditor
                    aceRef={promptRef}
                    control={control}
                    name="prompt"
                    mode="text"
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        {
                            component: <AceEditor.HelpAction message={<PromptSyntaxHelp />} />,
                            position: "bottom",
                        },
                    ]}
                    wrapEnabled
                    setOptions={{
                        indentedSoftWrap: false,
                    }}
                />
            </FormGroup>
            <SampleObjectAndSchemaFields
                control={control}
                setValue={setValue}
                sampleObjectName="sampleObject"
                sampleObject={formValues.sampleObject}
                sampleObjectSyntaxHelp={<SampleObjectSyntaxHelp />}
                jsonSchemaName="jsonSchema"
                jsonSchema={formValues.jsonSchema}
                jsonSchemaSyntaxHelp={<JsonSchemaSyntaxHelp />}
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

function PromptSyntaxHelp() {
    const samplePrompt =
        "Check if the following blog post comment is spam or not. A spam comment typically includes irrelevant or promotional content, excessive links, misleading information, or is written with the intent to manipulate search rankings or advertise products/services. Consider the language, intent, and relevance of the comment to the blog post topic. ";

    return (
        <div>
            <div>Sample prompt</div>
            <Code code={samplePrompt} elementToCopy={samplePrompt} language="plaintext" whiteSpace="normal" />
        </div>
    );
}

function SampleObjectSyntaxHelp() {
    const code = `{
    "IsCommentSpam": true,
    "Reason": "Concise reason for why this comment was marked as spam or ham"
}`;

    return (
        <div>
            <div>Sample response object</div>
            <Code code={code} elementToCopy={code} language="json" />
        </div>
    );
}

function JsonSchemaSyntaxHelp() {
    const code = `{
  "name": "some-name",
  "strict": true,
  "schema": {
    "type": "object",
    "properties": {
      "IsCommentSpam": {
        "type": "boolean"
      },
      "Reason": {
        "type": "string",
        "description": "Concise reason for why this comment was marked as spam or ham"
      }
    },
    "required": [
      "IsCommentSpam",
      "Reason"
    ],
    "additionalProperties": false
  }
}`;

    return (
        <div>
            <div>Sample JSON schema</div>
            <Code code={code} elementToCopy={code} language="json" />
        </div>
    );
}
