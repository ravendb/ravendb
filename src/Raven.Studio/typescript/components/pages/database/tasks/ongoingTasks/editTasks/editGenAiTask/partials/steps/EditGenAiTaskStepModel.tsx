import { useAppDispatch, useAppSelector } from "components/store";
import { Icon } from "components/common/Icon";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import EditGenAiTaskModelFields from "../fields/EditGenAiTaskModelFields";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useEditGenAiTaskTests } from "../../hooks/useEditGenAiTaskTests";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import EditGenAiTaskInfoHub from "../../EditGenAiTaskInfoHub";
import EditGenAiTaskCancelButton from "../EditGenAiTaskCancelButton";

export function EditGenAiTaskStepModel() {
    return (
        <>
            <div className="hstack justify-content-between">
                <AboutViewHeading title="Define prompt & JSON schema" marginBottom={2} icon="ai-etl" />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                The prompt and schema defined in this step will be used as input to the model for each context object
                generated in the previous step.
                <br />
                Use the playground to test the model&apos;s output on this input.
            </p>
            <EditGenAiTaskModelFields />
        </>
    );
}

export function EditGenAiTaskStepModelFooter() {
    const dispatch = useAppDispatch();

    const modelInputTest = useAppSelector(editGenAiTaskSelectors.modelInputTest);
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const { handleModelInputTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger(["prompt", "sampleObject", "jsonSchema"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("updateScript"));
        }
    };

    return (
        <div className="hstack justify-content-between">
            <div className="hstack gap-2">
                <EditGenAiTaskCancelButton />
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("context"))}
                >
                    <Icon icon="arrow-left" /> Back
                </Button>
            </div>
            <div className="hstack gap-2">
                <ConditionalPopover
                    conditions={[
                        {
                            isActive: formValues.playgroundContexts.length === 0,
                            message:
                                "To test the model output, either generate context objects in the previous step or enter custom ones here using Edit mode.",
                        },
                        {
                            isActive: true,
                            message: (
                                <>
                                    Click to test the model output using the combined input: context objects, prompt,
                                    and schema.
                                    <br />
                                    The resulting output object(s) will be shown in the results pane.
                                </>
                            ),
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleModelInputTest}
                        isSpinning={modelInputTest.status === "loading"}
                        icon="test"
                        disabled={formValues.playgroundContexts.length === 0}
                    >
                        Test model
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    Next <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </div>
        </div>
    );
}
