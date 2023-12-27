import React, { ReactNode } from "react";
import "./SortDropdown.scss";
import { Icon } from "./Icon";
import { DropdownMenu, DropdownToggle, UncontrolledDropdown } from "reactstrap";
import { Radio } from "./Checkbox";
import IconName from "typings/server/icons";

interface SortDropdownProps {
    label: ReactNode | ReactNode[];
    children: ReactNode | ReactNode[];
}

export function SortDropdown(props: SortDropdownProps) {
    const { label, children } = props;
    return (
        <UncontrolledDropdown className="sort-dropdown">
            <DropdownToggle className="sort-dropdown-toggle d-flex align-items-center" caret>
                <div className="flex-grow d-flex align-items-center">{label}</div>
            </DropdownToggle>
            <DropdownMenu className="py-4 px-5">
                <div className="hstack gap-5">{children}</div>
            </DropdownMenu>
        </UncontrolledDropdown>
    );
}

type SortableValue = string | number | boolean;

export interface sortItem<T extends SortableValue = string> {
    label: string;
    value: T;
    icon?: IconName;
}

interface SortDropdownRadioListProps<T extends SortableValue = string> {
    radioOptions: sortItem<T>[];
    label?: string;
    selected: T;
    setSelected: (value: T) => void;
}

export function SortDropdownRadioList<T extends SortableValue = string>(props: SortDropdownRadioListProps<T>) {
    const { radioOptions, label, selected, setSelected } = props;

    return (
        <div className="vstack gap-1 dropdown-radio-group">
            {label && <div className="small-label mb-2">{label}</div>}
            {radioOptions.map((item) => (
                <Radio
                    key={item.value.toString()}
                    selected={item.value === selected}
                    toggleSelection={() => setSelected(item.value)}
                >
                    {item.icon && <Icon icon={item.icon} />}
                    {item.label}
                </Radio>
            ))}
        </div>
    );
}
