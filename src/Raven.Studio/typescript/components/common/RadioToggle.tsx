import React from "react";
import classNames from "classnames";

interface ToggleProps {
    className?: string;
}

// TODO: add whole logic

export function RadioToggle({ className }: ToggleProps) {
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
