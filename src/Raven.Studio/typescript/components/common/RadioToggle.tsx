import React, { ReactNode, useEffect } from "react";
import classNames from "classnames";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";

export interface RadioToggleWithIconInputItem<T extends string | number | boolean = string> {
    label: string | ReactNode | ReactNode[];
    value: T;
    iconName: IconName;
}

export interface RadioToggleWithIconProps<T extends string | number | boolean = string> {
    name: string;
    leftItem: RadioToggleWithIconInputItem<T>;
    rightItem: RadioToggleWithIconInputItem<T>;
    selectedValue: T;
    setSelectedValue: (x: T) => void;
    className?: string;
    disabled?: boolean;
}

export function RadioToggleWithIcon<T extends string | number | boolean = string>({
    name,
    leftItem,
    rightItem,
    selectedValue,
    setSelectedValue,
    className,
    disabled,
}: RadioToggleWithIconProps<T>) {
    useEffect(() => {
        if (selectedValue == null) {
            setSelectedValue(leftItem.value);
        }
    }, [leftItem.value, selectedValue, setSelectedValue]);

    return (
        <div className={classNames("radio-toggle", className)}>
            <input
                type="radio"
                id="radio-toggle-left"
                name={name}
                checked={selectedValue === leftItem.value}
                onChange={() => setSelectedValue(leftItem.value)}
                disabled={disabled}
            />
            <label htmlFor="radio-toggle-left">{leftItem.label}</label>

            <input
                type="radio"
                id="radio-toggle-right"
                name={name}
                checked={selectedValue === rightItem.value}
                onChange={() => setSelectedValue(rightItem.value)}
                disabled={disabled}
            />
            <label htmlFor="radio-toggle-right">{rightItem.label}</label>

            <div className="toggle-knob">
                <Icon icon={leftItem.iconName} margin="m-0" />
                <Icon icon={rightItem.iconName} margin="m-0" />
            </div>
        </div>
    );
}
