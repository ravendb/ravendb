import "./WizardStepsList.scss";
import ProgressBar from "react-bootstrap/ProgressBar";
import ListStepItem from "components/common/ListStepItem";
import { NumberedList } from "components/common/NumberedList";
import { useStudioTranslation } from "hooks/useStudioTranslation";

export interface WizardStepsListItem<TStepId extends string> {
    id: TStepId;
    title: string;
}

interface WizardStepsListProps<TStepId extends string> {
    steps: WizardStepsListItem<TStepId>[];
    currentStepId: TStepId;
    onStepClick: (stepId: TStepId) => void;
}

export default function WizardStepsList<TStepId extends string>({
    steps,
    currentStepId,
    onStepClick,
}: WizardStepsListProps<TStepId>) {
    const currentStepIdx = steps.findIndex((x) => x.id === currentStepId);
    const lastStepIdx = steps.length - 1;
    const commonT = useStudioTranslation("common");

    return (
        <div className="wizard-steps-list vstack gap-3">
            <div className="wizard-steps-list-progress vstack gap-1">
                <span className="small">
                    {commonT("stepsCompleted", { completed: currentStepIdx, total: lastStepIdx })}
                </span>
                <ProgressBar now={currentStepIdx} max={lastStepIdx} variant="primary" />
            </div>
            <NumberedList>
                {steps.map((step, idx) => (
                    <ListStepItem
                        key={step.id}
                        isCurrent={idx === currentStepIdx}
                        isChecked={idx < currentStepIdx}
                        isInactive={idx > currentStepIdx}
                        className={idx > currentStepIdx ? "cursor-not-allowed" : "cursor-pointer"}
                        onClick={() => {
                            if (idx <= currentStepIdx) {
                                onStepClick(step.id);
                            }
                        }}
                    >
                        <h4 className="mb-0">
                            <button
                                type="button"
                                className="wizard-steps-list-title"
                                disabled={idx > currentStepIdx}
                                aria-current={idx === currentStepIdx ? "step" : undefined}
                            >
                                {step.title}
                            </button>
                        </h4>
                    </ListStepItem>
                ))}
            </NumberedList>
        </div>
    );
}
