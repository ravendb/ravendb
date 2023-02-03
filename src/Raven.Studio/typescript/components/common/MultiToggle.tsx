import React, { ReactNode } from "react";

import "./MultiToggle.scss";
import classNames from "classnames";

interface MultiToggleProps {
    children?: ReactNode | ReactNode[];
    className?: string;
}

export function RadioToggle(props: MultiToggleProps) {
    const { children, className } = props;
    return (
        <div className={classNames("radio-toggle", className)}>
            <input type="radio" id="radio-toggle-left-1" name="time-estimation" value="hourly" checked />
            <label htmlFor="radio-toggle-left-1">Bug</label>
            <input type="radio" id="radio-toggle-right-2" name="time-estimation" value="monthly" />
            <label htmlFor="radio-toggle-right-2">Feature</label>
            <div className="toggle-knob">
                <i className="icon-bug"></i>
                <i className="icon-experimental"></i>
            </div>
        </div>
    );
}

export function RadioMultiToggle(props: MultiToggleProps) {
    const { children, className } = props;

    return (
        <div className={classNames("multi-toggle", className)}>
            <div className="multi-toggle-item">
                <input id="1 hour" type="radio" name="timeSpan" value="1 hour" />
                <label htmlFor="1 hour">
                    <span>1 Hour</span>
                </label>
            </div>
            <div className="multi-toggle-item">
                <input id="6 hours" type="radio" name="timeSpan" value="6 hours" />
                <label htmlFor="6 hours">
                    <span>6 Hours</span>
                </label>
            </div>
            <div className="multi-toggle-item">
                <input id="12 hours" type="radio" name="timeSpan" value="12 hours" />
                <label htmlFor="12 hours">
                    <span>12 Hours</span>
                </label>
            </div>
            <div className="multi-toggle-item">
                <input id="1 day" type="radio" name="timeSpan" value="1 day" />
                <label htmlFor="1 day">
                    <span>1 Day</span>
                </label>
            </div>
        </div>
    );
}

export function CheckboxMultiToggle(props: MultiToggleProps) {
    const { children, className } = props;

    return (
        <div className={classNames("multi-toggle", className)}>
            <div className="multi-toggle-item">
                <input id="1hour" type="checkbox" name="timeSpan" value="1 hour" />
                <label htmlFor="1hour">
                    <span>Active</span>
                </label>
            </div>
            <div className="multi-toggle-item">
                <input id="6hours" type="checkbox" name="timeSpan" value="6 hours" />
                <label htmlFor="6hours">
                    <span>Paused</span>
                </label>
            </div>
            <div className="multi-toggle-item">
                <input id="12hours" type="checkbox" name="timeSpan" value="12 hours" />
                <label htmlFor="12hours">
                    <span>Error</span>
                </label>
            </div>
            <div className="multi-toggle-item">
                <input id="1day" type="checkbox" name="timeSpan" value="1 day" />
                <label htmlFor="1day">
                    <span>Stale</span>
                </label>
            </div>
        </div>
    );
}
