import React, { ReactNode } from "react";
import { UncontrolledDropdown, DropdownToggle, DropdownMenu, DropdownItem } from "reactstrap";
import IconName from "typings/server/icons";
import { Icon } from "./Icon";
import classNames from "classnames";

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
    placeholder?: string | ReactNode | ReactNode[];
    outline?: boolean;
    className?: string;
}

export default function Select<T extends string | number>(props: SelectProps<T>) {
    const { selectedValue, setSelectedValue, options, disabled, outline, placeholder, className } = props;

    const selectedOption = options.find((x) => x.value === selectedValue);

    return (
        <UncontrolledDropdown>
            <DropdownToggle
                caret
                disabled={disabled}
                outline={outline}
                className={classNames(className, "form-control select-btn")}
            >
                {selectedOption ? (
                    <>
                        {selectedOption.icon && <Icon icon={selectedOption.icon} />}
                        {selectedOption.label}
                    </>
                ) : (
                    <>{placeholder ? placeholder : "Select"}</>
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
