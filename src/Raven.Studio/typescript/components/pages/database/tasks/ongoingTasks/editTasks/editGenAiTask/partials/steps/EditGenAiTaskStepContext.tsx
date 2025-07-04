import EditGenAiTaskContextFields from "../fields/EditGenAiTaskContextFields";
import { useAppDispatch, useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";
import { editGenAiTaskActions, editGenAiTaskSelectors } from "../../store/editGenAiTaskSlice";
import { Icon } from "components/common/Icon";
import { useFormContext, useWatch } from "react-hook-form";
import { EditGenAiTaskFormData } from "../../utils/editGenAiTaskValidation";
import { AboutViewHeading } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useEditGenAiTaskTests } from "../../hooks/useEditGenAiTaskTests";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import EditGenAiTaskInfoHub from "../../EditGenAiTaskInfoHub";
import EditGenAiTaskCancelButton from "../EditGenAiTaskCancelButton";

export function EditGenAiTaskStepContext() {
    return (
        <>
            <div className="hstack justify-content-between">
                <AboutViewHeading title="Generate context objects" marginBottom={2} icon="genai" />
                <EditGenAiTaskInfoHub />
            </div>
            <p className="mb-4">
                The context objects generated in this step will be used as input for the model.
                <br />
                Each context object will be sent in a separate request, along with the prompt and JSON schema defined in
                the next step.
                <br />
                Use the playground to test the context generation script on a sample document.
            </p>
            <EditGenAiTaskContextFields />
        </>
    );
}

export function EditGenAiTaskStepContextFooter() {
    const dispatch = useAppDispatch();
    const { control, trigger } = useFormContext<EditGenAiTaskFormData>();
    const formValues = useWatch<EditGenAiTaskFormData>({ control });

    const contextTest = useAppSelector(editGenAiTaskSelectors.contextTest);

    const { handleContextTest } = useEditGenAiTaskTests();

    const handleNext = async () => {
        const isValid = await trigger(["collectionName", "script"]);

        if (isValid) {
            dispatch(editGenAiTaskActions.isTestOpenSet(false));
            dispatch(editGenAiTaskActions.currentStepSet("modelInput"));
        }
    };

    const isTestButtonDisabled = !formValues.playgroundDocument;

    return (
        <div className="hstack justify-content-between">
            <div className="hstack gap-2">
                <EditGenAiTaskCancelButton />
                <Button
                    variant="secondary"
                    className="rounded-pill"
                    onClick={() => dispatch(editGenAiTaskActions.currentStepSet("basic"))}
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
                                "To test the context generation script, select or provide a sample document in the playground.",
                        },
                        {
                            isActive: true,
                            message: (
                                <>
                                    Click to test the context generation script on the sample document.
                                    <br />
                                    The resulting context object(s) will be shown in the results pane.
                                </>
                            ),
                        },
                    ]}
                >
                    <ButtonWithSpinner
                        variant="info"
                        className="rounded-pill"
                        onClick={handleContextTest}
                        isSpinning={contextTest.status === "loading"}
                        disabled={isTestButtonDisabled}
                        icon="test"
                    >
                        Test context
                    </ButtonWithSpinner>
                </ConditionalPopover>

                <Button variant="primary" className="rounded-pill" onClick={handleNext}>
                    Next <Icon icon="arrow-right" margin="ms-1" />
                </Button>
            </div>
        </div>
    );
}
