import * as React from "react";
import classNames from "classnames";
import "./Steps.scss";
import { Icon } from "../Icon";
import { Spinner } from "reactstrap";

export interface StepItem {
    label: string;
    isInvalid?: boolean;
    isLoading?: boolean;
}

interface StepsProps {
    current: number;
    steps: StepItem[];
    onClick: (stepNum: number) => void;
    className?: string;
}

export default function Steps(props: StepsProps) {
    const { current, steps, onClick, className } = props;

    return (
        <div className={classNames("steps", className)}>
            {steps.map((step, idx) => (
                <Step
                    key={step.label}
                    step={step}
                    onClick={() => onClick(idx)}
                    isCurrent={idx === current}
                    isDone={current > idx}
                    isLast={idx === steps.length - 1}
                />
            ))}
        </div>
    );
}

interface StepProps {
    step: StepItem;
    onClick: () => void;
    isCurrent: boolean;
    isDone: boolean;
    isLast: boolean;
}

function Step({ step, onClick, isCurrent: isActive, isDone, isLast }: StepProps) {
    const classes = classNames({
        "steps-item": true,
        done: isDone,
        active: isActive,
    });

    return (
        <>
            <div className={classes} onClick={onClick}>
                <StepState step={step} />
                <span className={classNames("steps-label small-label", { "text-danger": step.isInvalid })}>
                    {step.label}
                </span>
            </div>
            {!isLast && <div className="steps-spacer" />}
        </>
    );
}

function StepState({ step }: { step: StepItem }) {
    if (step.isLoading) {
        return (
            <div className="step-bullet">
                <Spinner size="sm" className="m-0" color="primary" data-testid="loader" />
            </div>
        );
    }

    if (step.isInvalid) {
        return (
            <div className="step-bullet">
                <Icon icon="close" color="danger" className="step-invalid" margin="m-0" />
            </div>
        );
    }

    return (
        <div className="step-bullet">
            <Icon icon="arrow-thin-bottom" margin="m-0" className="bullet-icon-active" />
            <Icon icon="check" margin="m-0" className="bullet-icon-done" />
        </div>
    );
}
