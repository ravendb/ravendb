import { FormAceEditor, FormLabel, FormGroup, FormValidationMessage } from "components/common/Form";
import { useFormContext, useWatch } from "react-hook-form";
import { Icon } from "components/common/Icon";
import Badge from "react-bootstrap/Badge";
import { useRef, useState } from "react";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import Code from "components/common/Code";
import { useServices } from "components/hooks/useServices";
import { useAsyncCallback } from "react-async-hook";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";

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

    const { tasksService } = useServices();

    const [lastSampleObjectForGenerate, setLastSampleObjectForGenerate] = useState<string>("");

    const asyncGenerateSchema = useAsyncCallback(async () => {
        const result = await tasksService.getJsonSchemaFromSampleObject(JSON.parse(formValues.sampleObject));
        setValue("jsonSchema", result.Result, { shouldValidate: true });
        setLastSampleObjectForGenerate(formValues.sampleObject);
    });

    const canRegenerateSchema =
        !!formValues.sampleObject && !!formValues.jsonSchema && lastSampleObjectForGenerate !== formValues.sampleObject;

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
            <div>
                <div className="hstack gap-1">
                    <div className="vstack w-50">
                        <FormGroup className="vstack">
                            <FormLabel className="flex-grow-1 hstack justify-content-between align-items-start">
                                <div>
                                    Sample response object
                                    <PopoverWithHoverWrapper
                                        message={
                                            <>
                                                Enter a sample JSON object that defines the structure of the response
                                                you want to receive from the model.
                                                <br />
                                                <br />
                                                RavenDB will use this example to generate a{" "}
                                                <strong>formal JSON schema</strong>, which will be included in the
                                                request to the model.
                                            </>
                                        }
                                    >
                                        <Icon icon="info" color="info" margin="ms-1" />
                                    </PopoverWithHoverWrapper>
                                </div>
                                {!!formValues.sampleObject && !formValues.jsonSchema && (
                                    <Badge pill bg="info" style={{ whiteSpace: "normal" }}>
                                        Server will generate schema out of this object
                                    </Badge>
                                )}
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
                                                onLoad={(value) =>
                                                    setValue("sampleObject", value, { shouldValidate: true })
                                                }
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
                    </div>
                    <div className="vstack w-50">
                        <FormGroup className="vstack">
                            <FormLabel className="flex-grow-1 hstack justify-content-between align-items-start">
                                <div>
                                    JSON schema
                                    <PopoverWithHoverWrapper
                                        message={
                                            <>
                                                Enter a formal JSON schema that defines the structure of the response
                                                you want to receive from the model.
                                                <br />
                                                <br />
                                                If not provided, RavenDB will generate the schema automatically based on
                                                the sample response object.
                                            </>
                                        }
                                    >
                                        <Icon icon="info" color="info" margin="ms-1" />
                                    </PopoverWithHoverWrapper>
                                </div>
                                {!!formValues.jsonSchema && (
                                    <Badge pill bg="info" style={{ whiteSpace: "normal" }}>
                                        Server will use this schema to communicate with AI
                                    </Badge>
                                )}
                            </FormLabel>
                            <div className="position-relative">
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
                                                    onLoad={(value) =>
                                                        setValue("jsonSchema", value, { shouldValidate: true })
                                                    }
                                                />
                                            ),
                                        },
                                        {
                                            component: <AceEditor.HelpAction message={<JsonSchemaSyntaxHelp />} />,
                                            position: "bottom",
                                        },
                                    ]}
                                />
                                {!!formValues.sampleObject && !formValues.jsonSchema && (
                                    <ButtonWithSpinner
                                        className="rounded-pill position-absolute top-50 start-50 translate-middle"
                                        variant="primary"
                                        onClick={asyncGenerateSchema.execute}
                                        isSpinning={asyncGenerateSchema.loading}
                                        icon="refresh"
                                    >
                                        Generate schema
                                    </ButtonWithSpinner>
                                )}
                                {canRegenerateSchema && (
                                    <ButtonWithSpinner
                                        className="rounded-pill position-absolute"
                                        style={{
                                            bottom: "20px",
                                            right: "54px",
                                        }}
                                        variant="primary"
                                        onClick={asyncGenerateSchema.execute}
                                        isSpinning={asyncGenerateSchema.loading}
                                        icon="refresh"
                                    >
                                        Re-generate schema
                                    </ButtonWithSpinner>
                                )}
                            </div>
                        </FormGroup>
                    </div>
                </div>
                {errors.schemaProvider && (
                    <FormValidationMessage>{errors.schemaProvider?.message.toString()}</FormValidationMessage>
                )}
            </div>
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
