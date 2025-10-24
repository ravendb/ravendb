import { FormAceEditor, FormLabel, FormGroup } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { useRef } from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import Code from "components/common/Code";
import SampleObjectAndSchemaFields from "components/common/sampleObjectAndSchemaFields/SampleObjectAndSchemaFields";
import useBoolean from "components/hooks/useBoolean";
import AiAssistantWindow from "components/common/aiAssistant/AiAssistantWindow";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import AiAssistantButton from "components/common/aiAssistant/AiAssistantButton";

export default function EditGenAiTaskModelFields() {
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();

    const formValues = useWatch({ control });

    const promptRef = useRef<ReactAce>(null);

    const { value: isAiAssistOpen, toggle: toggleIsAiAssistOpen } = useBoolean(false);

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
                                component: <AceEditor.HelpAction message={<PromptSyntaxHelp />} />,
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
                        />
                    )}
                </div>
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
