import React, { ComponentProps } from "react";
import ReactSelect, { GroupBase, OptionProps, SingleValueProps, components, MultiValueProps } from "react-select";
import { Icon } from "../Icon";
import "./Select.scss";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";

export interface SelectOption<T extends string | number = string> {
    value: T;
    label: string;
    icon?: IconName;
    iconColor?: TextColor;
    horizontalSeparatorLine?: boolean;
}

export default function Select<
    Option extends SelectOption<string | number>,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>
>(props: ComponentProps<typeof ReactSelect<Option, IsMulti, Group>>) {
    return (
        <ReactSelect
            {...props}
            className="bs5 react-select-container"
            classNamePrefix="react-select"
            components={SelectCommonComponents}
        />
    );
}

export function Option(props: OptionProps<SelectOption<string | number>>) {
    const { data } = props;

    return (
        <div style={{ cursor: "default" }}>
            <components.Option {...props}>
                {data.icon && <Icon icon={data.icon} color={data.iconColor} />}
                {data.label}
            </components.Option>
            {data.horizontalSeparatorLine && <hr />}
        </div>
    );
}

export function SingleValue({ children, ...props }: SingleValueProps<SelectOption<string | number>>) {
    return (
        <components.SingleValue {...props}>
            {props.data.icon && <Icon icon={props.data.icon} color={props.data.iconColor} />}
            {children}
        </components.SingleValue>
    );
}

export function MultiValueLabel({ children, ...props }: MultiValueProps<SelectOption<string | number>>) {
    return (
        <components.MultiValueLabel {...props}>
            {props.data.icon && <Icon icon={props.data.icon} color={props.data.iconColor} />}
            {children}
        </components.MultiValueLabel>
    );
}

export const SelectCommonComponents = {
    Option,
    SingleValue,
    MultiValueLabel,
};
