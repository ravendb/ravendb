import React, { ReactNode } from "react";
import useId from "hooks/useId";
import "./SortDropdown.scss";
import { Icon } from "./Icon";
import { DropdownMenu, DropdownToggle, UncontrolledDropdown } from "reactstrap";
import { Radio } from "./Checkbox";
import IconName from "typings/server/icons";

interface SortDropdownProps {
    label: string | ReactNode | ReactNode[];
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

interface SortDropdownRadioListProps {
    radioOptions: sortItem[];
    label?: string;
    selected: string;
    setSelected: (value: string) => void;
}

export function SortDropdownRadioList<T extends string | number = string>(props: SortDropdownRadioListProps) {
    const { radioOptions, label, selected, setSelected } = props;
    const uniqueId = useId("sort-dropdown-list");

    return (
        <div className="vstack gap-1 dropdown-radio-group">
            {label && <div className="small-label mb-2">{label}</div>}
            {radioOptions.map((item) => (
                <Radio
                    key={uniqueId + item.value}
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

export interface sortItem<T extends string | number = string> {
    label: string;
    value: T;
    icon?: IconName;
}
