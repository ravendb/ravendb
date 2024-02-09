import * as React from "react";
import classNames from "classnames";
import "./Steps.scss";
import { Icon } from "./Icon";
import { todo } from "common/developmentHelper";

export interface StepItem {
    label: string;
    isInvalid?: boolean;
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
                    isDone={idx < current}
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

    todo("Styling", "Kwiato", "invalid state");
    return (
        <>
            <div className={classes} onClick={onClick}>
                {step.isInvalid ? (
                    <div>
                        <Icon icon="cancel" color="danger" margin="m-0" />
                    </div>
                ) : (
                    <div className="step-bullet">
                        <Icon icon="arrow-thin-bottom" margin="m-0" className="bullet-icon-active" />
                        <Icon icon="check" margin="m-0" className="bullet-icon-done" />
                    </div>
                )}
                <span className={classNames("steps-label small-label", { "text-danger": step.isInvalid })}>
                    {step.label}
                </span>
            </div>
            {!isLast && <div className="steps-spacer" />}
        </>
    );
}
