import * as React from "react";
import classNames from "classnames";

import "./Steps.scss";
interface Props {
    current: number;
    steps: string[];
    className?: string;
}

export class Steps extends React.Component<Props> {
    render() {
        const { current, steps, className } = this.props;

        const stepNodes = steps.map((stepName, i) => {
            const classes = classNames({
                "steps-item": true,
                done: i < current,
                active: i === current,
            });
            const stepItem = (
                <div key={"step-" + i} className={classes}>
                    <div className="step-bullet">
                        <i className="icon-arrow-thin-bottom bullet-icon-active" />
                        <i className="icon-check bullet-icon-done" />
                    </div>
                    <span className="steps-label small-label">{stepName}</span>
                </div>
            );

            const spacer = <div className="steps-spacer" />;

            return (
                <React.Fragment key={"step-" + i}>
                    {stepItem}
                    {i !== steps.length - 1 && spacer}
                </React.Fragment>
            );
        });

        return <div className={classNames("steps", className)}>{stepNodes}</div>;
    }
}
