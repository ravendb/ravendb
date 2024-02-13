import { useState } from "react";

export function useSteps(stepsCount: number) {
    const [currentStep, setCurrentStep] = useState(0);

    const isFirstStep = currentStep === 0;
    const isLastStep = currentStep === stepsCount - 1;

    const goToStep = (stepNum: number) => {
        setCurrentStep(stepNum);
    };

    const nextStep = () => {
        if (!isLastStep) {
            setCurrentStep((step) => step + 1);
        }
    };

    const prevStep = () => {
        if (!isFirstStep) {
            setCurrentStep((step) => step - 1);
        }
    };

    return {
        currentStep,
        isFirstStep,
        isLastStep,
        goToStep,
        nextStep,
        prevStep,
    };
}
