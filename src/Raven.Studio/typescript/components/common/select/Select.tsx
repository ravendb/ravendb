import React, { ComponentProps } from "react";
import ReactSelect, {
    GroupBase,
    OptionProps,
    SingleValueProps,
    components,
    MultiValueProps,
    InputProps,
    StylesConfig,
} from "react-select";
import { Icon } from "../Icon";
import "./Select.scss";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";
import classNames from "classnames";

export type SelectValue = string | number | boolean;

export interface SelectOption<T = string> {
    value: T;
    label: string;
}

export interface SelectOptionIcon {
    icon?: IconName;
    iconColor?: TextColor;
}

export interface SelectOptionWarning {
    isWarning?: boolean;
}

export interface SelectOptionSeparator {
    horizontalSeparatorLine?: boolean;
}

export type SelectOptionWithIcon<T extends SelectValue = string> = SelectOption<T> & SelectOptionIcon;
export type SelectOptionWithWarning<T extends SelectValue = string> = SelectOption<T> & SelectOptionWarning;
export type SelectOptionWithIconAndSeparator<T extends SelectValue = string> = SelectOptionWithIcon<T> &
    SelectOptionSeparator;

interface SelectProps<Option, IsMulti extends boolean = false, Group extends GroupBase<Option> = GroupBase<Option>>
    extends ComponentProps<typeof ReactSelect<Option, IsMulti, Group>> {
    isRoundedPill?: boolean;
}

export default function Select<
    Option,
    IsMulti extends boolean = false,
    Group extends GroupBase<Option> = GroupBase<Option>,
>({ isRoundedPill, className, styles = {}, ...rest }: SelectProps<Option, IsMulti, Group>) {
    if (isRoundedPill) {
        applyRoundedPillStyle(styles);
    }

    return (
        <ReactSelect
            styles={styles}
            {...rest}
            classNamePrefix="react-select"
            className={classNames("bs5 react-select-container", { "rounded-pill": isRoundedPill }, className)}
        />
    );
}

export function OptionWithIcon(props: OptionProps<SelectOptionWithIcon>) {
    const { data } = props;

    return (
        <div className="cursor-pointer">
            <components.Option {...props}>
                {data.icon && <Icon icon={data.icon} color={data.iconColor} />}
                {data.label}
            </components.Option>
        </div>
    );
}

export function OptionWithWarning(props: OptionProps<SelectOptionWithWarning>) {
    const { data } = props;

    return (
        <div className="cursor-pointer">
            <components.Option {...props} className={classNames({ "text-warning": data.isWarning })}>
                {data.isWarning && <Icon icon="warning" color="warning" />}
                {data.label}
            </components.Option>
        </div>
    );
}

export function OptionWithIconAndSeparator(props: OptionProps<SelectOptionWithIconAndSeparator>) {
    const { data } = props;

    return (
        <div className="cursor-pointer">
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

// https://github.com/JedWatson/react-select/issues/3068
export function InputNotHidden({ ...props }: InputProps) {
    return <components.Input {...props} isHidden={false} />;
}

export function applyRoundedPillStyle(styles: StylesConfig) {
    styles.control = (base) => ({
        ...base,
        borderRadius: "50rem",
    });
}
