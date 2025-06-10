import { FormAceEditor, FormLabel, FormGroup, FormValidationMessage } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import Badge from "react-bootstrap/Badge";
import { ReactNode, useRef } from "react";
import IconName from "typings/server/icons";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import Code from "components/common/Code";

export default function EditGenAiTaskModelFields() {
    const {
        control,
        setValue,
        formState: { errors },
    } = useFormContext();

    const formValues = useWatch({ control });

    const promptRef = useRef<ReactAce>(null);
    const sampleObjectRef = useRef<ReactAce>(null);
    const jsonSchemaRef = useRef<ReactAce>(null);

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
            {formValues.schemaProvider == null && (
                <div>
                    <div className="mb-1">
                        JSON schema
                        <PopoverWithHoverWrapper message={<JsonSchemaTooltipBody />}>
                            <Icon icon="info" color="info" margin="ms-1" />
                        </PopoverWithHoverWrapper>
                    </div>
                    <div className="hstack gap-1">
                        <div className="flex-grow-1 vstack w-50">
                            <SchemaProviderButton
                                icon="default"
                                title={
                                    <>
                                        Provide sample response object <Badge bg="faded-success">Recommended</Badge>
                                    </>
                                }
                                description="Use this option if you want RavenDB to automatically generate a JSON schema for you"
                                handleClick={() => setValue("schemaProvider", "sampleObject")}
                            />
                        </div>
                        <div className="flex-grow-1 vstack w-50">
                            <SchemaProviderButton
                                icon="edit"
                                title="Provide JSON schema"
                                description="Use this option if you want to provide your own JSON schema"
                                handleClick={() => setValue("schemaProvider", "jsonSchema")}
                            />
                        </div>
                    </div>
                    {errors.schemaProvider && (
                        <FormValidationMessage>{errors.schemaProvider?.message.toString()}</FormValidationMessage>
                    )}
                </div>
            )}
            {formValues.schemaProvider === "sampleObject" && (
                <FormGroup>
                    <FormLabel className="hstack justify-content-between">
                        <div>
                            Sample response object
                            <PopoverWithHoverWrapper
                                message={
                                    <>
                                        Enter a sample JSON object that defines the structure of the response you want
                                        to receive from the model.
                                        <br />
                                        <br />
                                        RavenDB will use this example to generate a <strong>formal JSON schema</strong>,
                                        which will be included in the request to the model.
                                    </>
                                }
                            >
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </div>
                        <Button variant="link" size="xs" onClick={() => setValue("schemaProvider", "jsonSchema")}>
                            <Icon icon="edit" />
                            Provide JSON schema
                        </Button>
                    </FormLabel>
                    <FormAceEditor
                        aceRef={sampleObjectRef}
                        control={control}
                        name="sampleObject"
                        mode="json"
                        actions={[
                            { component: <AceEditor.FullScreenAction /> },
                            { component: <AceEditor.FormatAction /> },
                            {
                                component: (
                                    <AceEditor.LoadFileAction
                                        onLoad={(value) => setValue("sampleObject", value, { shouldValidate: true })}
                                    />
                                ),
                            },
                            {
                                component: <AceEditor.HelpAction message={<SampleObjectSyntaxHelp />} />,
                                position: "bottom",
                            },
                        ]}
                    />
                </FormGroup>
            )}
            {formValues.schemaProvider === "jsonSchema" && (
                <FormGroup>
                    <FormLabel className="hstack justify-content-between">
                        <div>
                            JSON schema
                            <PopoverWithHoverWrapper message={<JsonSchemaTooltipBody />}>
                                <Icon icon="info" color="info" margin="ms-1" />
                            </PopoverWithHoverWrapper>
                        </div>
                        <Button variant="link" size="xs" onClick={() => setValue("schemaProvider", "sampleObject")}>
                            <Icon icon="default" />
                            Provide sample response object
                        </Button>
                    </FormLabel>
                    <FormAceEditor
                        aceRef={jsonSchemaRef}
                        control={control}
                        name="jsonSchema"
                        mode="json"
                        actions={[
                            { component: <AceEditor.FullScreenAction /> },
                            { component: <AceEditor.FormatAction /> },
                            {
                                component: (
                                    <AceEditor.LoadFileAction
                                        onLoad={(value) => setValue("jsonSchema", value, { shouldValidate: true })}
                                    />
                                ),
                            },
                            {
                                component: <AceEditor.HelpAction message={<JsonSchemaSyntaxHelp />} />,
                                position: "bottom",
                            },
                        ]}
                    />
                </FormGroup>
            )}
        </>
    );
}

interface SchemaProviderButtonProps {
    icon: IconName;
    title: ReactNode;
    description: ReactNode;
    handleClick: () => void;
}

function SchemaProviderButton({ icon, title, description, handleClick }: SchemaProviderButtonProps) {
    return (
        <div className="border border-secondary rounded p-2 cursor-pointer h-100 flex-grow-1" onClick={handleClick}>
            <div className="text-emphasis hstack gap-2 h-100">
                <div>
                    <Icon icon={icon} margin="m-0" style={{ fontSize: 24 }} />
                </div>
                <div className="flex-grow">
                    <h4 className="mb-1">{title}</h4>
                    <span>{description}</span>
                </div>
            </div>
        </div>
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

function JsonSchemaTooltipBody() {
    return (
        <>
            Enter a JSON schema that defines the structure of the output you expect from the model.
            <br />
            <br />
            If not provided, RavenDB will generate the schema automatically based on the sample response object.
        </>
    );
}
