import React from "react";
import { UncontrolledDropdown, DropdownToggle, DropdownMenu, DropdownItem } from "reactstrap";
import IconName from "typings/server/icons";
import { Icon } from "./Icon";
import { todo } from "common/developmentHelper";

export interface SelectOption<T extends string | number> {
    value: T;
    label: string;
    icon?: IconName;
    horizontalSeparatorLine?: boolean;
}

export interface SelectProps<T extends string | number> {
    selectedValue?: T;
    setSelectedValue: (x: T) => void;
    options: SelectOption<T>[];
    disabled?: boolean;
}

export default function Select<T extends string | number>(props: SelectProps<T>) {
    const { selectedValue, setSelectedValue, options, disabled } = props;

    const selectedOption = options.find((x) => x.value === selectedValue);

    todo("Styling", "Kwiato", "styles for disabled status");
    todo("Styling", "Kwiato", "replace '-- Select --' text when nothing is selected");

    return (
        <UncontrolledDropdown disabled={disabled}>
            <DropdownToggle caret>
                {selectedOption ? (
                    <>
                        {selectedOption.icon && <Icon icon={selectedOption.icon} />}
                        {selectedOption.label}
                    </>
                ) : (
                    "-- Select --"
                )}
            </DropdownToggle>
            <DropdownMenu>
                {options.map((option) => (
                    <React.Fragment key={option.value}>
                        <DropdownItem onClick={() => setSelectedValue(option.value)}>
                            {option.icon && <Icon icon={option.icon} />}
                            {option.label}
                        </DropdownItem>
                        {option.horizontalSeparatorLine && <DropdownItem divider />}
                    </React.Fragment>
                ))}
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}
