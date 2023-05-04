import React, { HTMLAttributes } from "react";
import classNames from "classnames";
import IconName from "typings/server/icons";

export interface IconProps extends HTMLAttributes<HTMLDivElement> {
    icon: IconName;
    addon?: IconName;
    color?: string;
    margin?: string;
    className?: string;
}

export function Icon(props: IconProps) {
    const { icon, addon, color, margin, className, ...rest } = props;
    const iconClasses = "icon-" + icon;
    const addonClasses = addon ? "icon-addon-" + addon : null;
    const colorClasses = color ? "text-" + color : null;
    const marginClass = margin ?? "me-1";
    return <i className={classNames(iconClasses, addonClasses, colorClasses, marginClass, className)} {...rest} />;
}
