import React, { HTMLAttributes } from "react";
import classNames from "classnames";
import IconName from "typings/server/icons";
import { TextColor } from "components/models/common";

export type IconSize = "xs" | "sm" | "md" | "lg" | "xl";

export interface IconProps extends HTMLAttributes<HTMLDivElement> {
    icon: IconName;
    addon?: IconName;
    color?: TextColor;
    margin?: string;
    className?: string;
    size?: IconSize;
}

export function Icon(props: IconProps) {
    const { icon, addon, color, margin, className, size, ...rest } = props;
    const iconClasses = "icon-" + icon;
    const addonClasses = addon ? "icon-addon-" + addon : null;
    const colorClasses = color ? "text-" + color : null;
    const marginClass = margin ?? "me-1";
    const sizeClass = size ? "icon-" + size : "icon-md";

    return (
        <i
            className={classNames(iconClasses, addonClasses, colorClasses, marginClass, className, sizeClass)}
            {...rest}
        />
    );
}
