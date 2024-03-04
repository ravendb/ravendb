import { useState } from "react";

interface ValidationResult {
    isValid: boolean;
}

export interface StepInRangeValidationResult extends ValidationResult {
    invalidStep?: number;
}

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

    const goToStepWithValidation = async (
        step: number,
        validateStepsInRange: () => Promise<StepInRangeValidationResult>
    ) => {
        if (step <= currentStep) {
            goToStep(step);
        }

        const result = await validateStepsInRange();

        if (result.isValid) {
            goToStep(step);
        } else {
            goToStep(result.invalidStep);
        }
    };

    const nextStepWithValidation = async (validateStep: () => Promise<ValidationResult>) => {
        const result = await validateStep();

        if (result.isValid) {
            nextStep();
        }
    };

    return {
        currentStep,
        isFirstStep,
        isLastStep,
        goToStep,
        nextStep,
        prevStep,
        goToStepWithValidation,
        nextStepWithValidation,
    };
}
