import WizardStepsList from "components/common/wizard/WizardStepsList";
import { useAppDispatch } from "components/store";
import { EditGenAiTaskStep } from "../hooks/useEditGenAiTaskSteps";
import { editGenAiTaskActions } from "../store/editGenAiTaskSlice";

interface EditGenAiTaskStepsProps {
    steps: EditGenAiTaskStep[];
}

export default function EditGenAiTaskSteps({ steps }: EditGenAiTaskStepsProps) {
    const dispatch = useAppDispatch();

    return (
        <WizardStepsList
            steps={steps}
            currentStepId={steps.find((x) => x.isCurrent).id}
            onStepClick={(stepId) => dispatch(editGenAiTaskActions.currentStepSet(stepId))}
        />
    );
}
