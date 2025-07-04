import { FormGroup, FormLabel } from "components/common/Form";
import { FormAceEditor } from "components/common/Form";
import { useAppDispatch, useAppSelector } from "components/store";
import { useFormContext, useWatch } from "react-hook-form";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import { useEditGenAiTaskTests } from "../../hooks/useEditGenAiTaskTests";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import EditGenAiTaskInfoHub from "../../EditGenAiTaskInfoHub";
import AceEditor from "components/common/ace/AceEditor";
import ReactAce from "react-ace";
import { useRef } from "react";
import Code from "components/common/Code";
import EditGenAiTaskCancelButton from "../EditGenAiTaskCancelButton";

export function EditGenAiTaskStepUpdate() {
    const { control, setValue } = useFormContext<EditGenAiTaskFormData>();

    const scriptRef = useRef<ReactAce>(null);

    return (
        <>
            <div className="hstack justify-content-between">
                <AboutViewHeading title="Provide update script" marginBottom={2} icon="genai" />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                The &quot;update script&quot; provided in this step will be used to modify your source documents based
                on the content of the model’s output objects.
                <br />
                Use the playground to test the effect of the script on a sample source document.
            </p>
            <FormGroup>
                <FormLabel>
                    Update script
                    <PopoverWithHoverWrapper
                        message={
                            <>
                                Provide a script to update your source documents using the model&apos;s response.
                                <br />
                                You can refer to <code>$input</code> (the context object) and <code>$output</code> (the
                                model&apos;s response object) within the script.
                            </>
                        }
                    >
                        <Icon icon="info" color="info" margin="ms-1" />
                    </PopoverWithHoverWrapper>
                </FormLabel>
                <FormAceEditor
                    aceRef={scriptRef}
                    control={control}
                    name="updateScript"
                    mode="javascript"
                    actions={[
                        { component: <AceEditor.FullScreenAction /> },
                        { component: <AceEditor.FormatAction /> },
                        {
                            component: (
                                <AceEditor.LoadFileAction
                                    onLoad={(value) => setValue("updateScript", value, { shouldValidate: true })}
                                />
                            ),
                        },
                        {
                            component: <AceEditor.HelpAction message={<UpdateScriptSyntaxHelp />} />,
                            position: "bottom",
                        },
                    ]}
                />
            </FormGroup>
        </>
    );
}

export function EditGenAiTaskStepUpdateFooter() {
    const dispatch = useAppDispatch();
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });
    const updateScriptTest = useAppSelector(editGenAiTaskSelectors.updateScriptTest);

    const { handleUpdateScriptTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger(["updateScript"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("summary"));
        }
    };

    const isTestButtonDisabled =
        !formValues.playgroundDocument ||
        formValues.playgroundContexts.length === 0 ||
        formValues.playgroundModelOutputs.length === 0 ||
        formValues.playgroundContexts.length !== formValues.playgroundModelOutputs.length;

    console.log("kalczur", {
        playgroundContexts: formValues.playgroundContexts,
        playgroundModelOutputs: formValues.playgroundModelOutputs,
    });

    return (
        <div className="hstack justify-content-between">
            <div className="hstack gap-2">
                <EditGenAiTaskCancelButton />
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("modelInput"))}
                >
                    <Icon icon="arrow-left" /> Back
                </Button>
            </div>
            <div className="hstack gap-2">
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: !formValues.playgroundDocument,
                            message:
                                "To test the the update script, select or provide a sample document in the playground.",
                        },
                        {
                            isActive: formValues.playgroundContexts.length === 0,
                            message:
                                "To test the update script, either generate context objects in the 'Generate context objects' step or enter custom ones using Edit mode.",
                        },
                        {
                            isActive: formValues.playgroundModelOutputs.length === 0,
                            message:
                                "To test the update script, either generate model output objects in the previous step or enter custom ones here using Edit mode.",
                        },
                        {
                            isActive: formValues.playgroundContexts.length !== formValues.playgroundModelOutputs.length,
                            message:
                                "To test the update script, the number of context objects and model output objects must be the same.",
                        },
                        {
                            isActive: true,
                            message: (
                                <>
                                    Click to test the update script.
                                    <br />
                                    The updated sample source document will be shown in the results pane.
                                </>
                            ),
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleUpdateScriptTest}
                        isSpinning={updateScriptTest.status === "loading"}
                        icon="test"
                        disabled={isTestButtonDisabled}
                    >
                        Test update script
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    Next <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </div>
        </div>
    );
}

function UpdateScriptSyntaxHelp() {
    const code = `// $input - the model input (generated by ai.genContext())
// $output - the model output
// this - the source document

const idx = this.Comments.findIndex(comment => comment.Id == $input.CommentId);
if ($output.IsCommentSpam) {
    // Update the source document:
    this.Comments.splice(idx, 1); // remove comment
    // Can create new documents:
    const newDocument = { "comment": $input.Text, "@metadata": { "@collection": "spamComments"} };
    put(id(this) +"/spam/", newDocument);
}`;

    return (
        <div>
            <div>Sample update script</div>
            <Code code={code} elementToCopy={code} language="javascript" />
        </div>
    );
}
