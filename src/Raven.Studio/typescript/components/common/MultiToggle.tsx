import React, { ReactNode } from "react";

import "./MultiToggle.scss";
import useId from "hooks/useId";
import classNames from "classnames";

export interface InputItem {
    label: string;
    value: string;
}

interface ToggleProps {
    className?: string;
}

export function RadioToggle(props: ToggleProps) {
    const { className } = props;
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

interface MultiToggleProps {
    inputList: InputItem[];
    className?: string;
    radio?: boolean;
    label?: string;
}

export function MultiToggle(props: MultiToggleProps) {
    const { inputList, className, radio, label } = props;

    const uniqueId = useId("multi-toggle");

    return (
        <div className={classNames("multi-toggle", className)}>
            {label && <div className="small-label ms-1 mb-1">{label}</div>}
            <div className="multi-toggle-list">
                {inputList.map((inputItem) => (
                    <div className="multi-toggle-item">
                        <input
                            id={uniqueId + inputItem.value}
                            type={radio ? "radio" : "checkbox"}
                            name={uniqueId}
                            value={inputItem.value}
                        />
                        <label htmlFor={uniqueId + inputItem.value}>
                            <span>{inputItem.label}</span>
                        </label>
                    </div>
                ))}
            </div>
        </div>
    );
}
