import React from "react";
import classNames from "classnames";
import { Icon } from "./Icon";

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
                <Icon icon="bug" margin="m-0" />
                <Icon icon="experimental" margin="m-0" />
            </div>
        </div>
    );
}
