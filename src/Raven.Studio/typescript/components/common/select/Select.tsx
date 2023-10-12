import React, { ComponentProps } from "react";
import ReactSelect, { GroupBase, OptionProps, SingleValueProps, components, MultiValueProps } from "react-select";
import { Icon } from "../Icon";
import "./Select.scss";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";

export type SelectValue = string | number | boolean;

export interface SelectOption<T extends SelectValue = string> {
    value: T;
    label: string;
}

export interface SelectOptionIcon {
    icon: IconName;
    iconColor?: TextColor;
}

export interface SelectOptionSeparator {
    horizontalSeparatorLine?: boolean;
}

export type SelectOptionWithIcon<T extends SelectValue = string> = SelectOption<T> & SelectOptionIcon;
export type SelectOptionWithIconAndSeparator<T extends SelectValue = string> = SelectOptionWithIcon<T> &
    SelectOptionSeparator;

export default function Select<
    Option,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>
>(props: ComponentProps<typeof ReactSelect<Option, IsMulti, Group>>) {
    return <ReactSelect {...props} className="bs5 react-select-container" classNamePrefix="react-select" />;
}

export function OptionWithIcon(props: OptionProps<SelectOptionWithIcon>) {
    const { data } = props;

    return (
        <div style={{ cursor: "default" }}>
            <components.Option {...props}>
                {data.icon && <Icon icon={data.icon} color={data.iconColor} />}
                {data.label}
            </components.Option>
        </div>
    );
}

export function OptionWithIconAndSeparator(props: OptionProps<SelectOptionWithIconAndSeparator>) {
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

export function SingleValueWithIcon({ children, ...props }: SingleValueProps<SelectOptionWithIcon>) {
    return (
        <components.SingleValue {...props}>
            {props.data.icon && <Icon icon={props.data.icon} color={props.data.iconColor} />}
            {children}
        </components.SingleValue>
    );
}

export function MultiValueLabelWithIcon({ children, ...props }: MultiValueProps<SelectOptionWithIcon>) {
    return (
        <components.MultiValueLabel {...props}>
            {props.data.icon && <Icon icon={props.data.icon} color={props.data.iconColor} />}
            {children}
        </components.MultiValueLabel>
    );
}
