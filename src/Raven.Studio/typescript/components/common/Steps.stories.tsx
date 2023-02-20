import { ComponentMeta } from "@storybook/react";
import { Steps } from "./Steps";
import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Button, Card } from "reactstrap";
import { FlexGrow } from "./FlexGrow";
import classNames from "classnames";

export default {
    title: "Bits/Steps",
    component: Steps,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof Steps>;

export function StepsExample() {
    const [currentStep, setCurrentStep] = useState(0);
    const stepsList = ["Setup", "Encryption", "Replication & Sharding", "Manual Node Selection", "Paths Configuration"];

    const isLastStep = stepsList.length - 2 < currentStep;
    const isFirstStep = currentStep < 1;

    const goToStep = (stepNum: number) => {
        setCurrentStep(stepNum);
    };

    const nextStep = () => {
        if (!isLastStep) setCurrentStep(currentStep + 1);
    };

    const prevStep = () => {
        if (!isFirstStep) setCurrentStep(currentStep - 1);
    };

    return (
        <Card className="p-4">
            <h1>Steps</h1>
            <Steps current={currentStep} steps={stepsList} onClick={goToStep} className="mb-4"></Steps>
            <div className="lead d-flex justify-content-center align-items-center">
                <div className="m-3">
                    Current step: <strong>{currentStep + 1}</strong> / {stepsList.length}
                </div>
                |
                <div className="m-3">
                    First step:{" "}
                    {isFirstStep ? (
                        <strong className="text-success">True</strong>
                    ) : (
                        <strong className="text-danger">False</strong>
                    )}
                </div>
                |
                <div className="m-3">
                    Last step:{" "}
                    {isLastStep ? (
                        <strong className="text-success">True</strong>
                    ) : (
                        <strong className="text-danger">False</strong>
                    )}
                </div>
            </div>
            <div className="d-flex my-4">
                {!isFirstStep && (
                    <Button onClick={prevStep}>
                        <i className="icon-arrow-left" /> Back
                    </Button>
                )}
                <FlexGrow />
                {isLastStep ? (
                    <Button color="success">
                        <i className="icon-rocket me-1" /> Finish
                    </Button>
                ) : (
                    <Button color="primary" onClick={nextStep} disabled={isLastStep}>
                        Next <i className="icon-arrow-right" />
                    </Button>
                )}
            </div>
        </Card>
    );
}
