import React from "react";
import classNames from "classnames";
import { Icon } from "./Icon";
import { Input } from "reactstrap";
import IconName from "typings/server/icons";

export interface RadioToggleWithIconInputItem<T extends string | number = string> {
    label: string;
    value: T;
    iconName: IconName;
}

interface RadioToggleWithIconProps {
    name: string;
    leftItem: RadioToggleWithIconInputItem;
    rightItem: RadioToggleWithIconInputItem;
    selectedItem: RadioToggleWithIconInputItem;
    setSelectedItem: (x: RadioToggleWithIconInputItem) => void;
    className?: string;
}

export function RadioToggleWithIcon({
    name,
    leftItem,
    rightItem,
    selectedItem,
    setSelectedItem,
    className,
}: RadioToggleWithIconProps) {
    return (
        <div className={classNames("radio-toggle", className)}>
            <input
                type="radio"
                id="radio-toggle-left"
                name={name}
                value={leftItem.value}
                checked={selectedItem.value === leftItem.value}
                onChange={() => setSelectedItem(leftItem)}
            />
            <label htmlFor="radio-toggle-left">{leftItem.label}</label>

            <input
                type="radio"
                id="radio-toggle-right"
                name={name}
                value={rightItem.value}
                checked={selectedItem.value === rightItem.value}
                onChange={() => setSelectedItem(rightItem)}
            />
            <label htmlFor="radio-toggle-right">{rightItem.label}</label>

            <div className="toggle-knob">
                <Icon icon={leftItem.iconName} margin="m-0" />
                <Icon icon={rightItem.iconName} margin="m-0" />
            </div>
        </div>
    );
}
