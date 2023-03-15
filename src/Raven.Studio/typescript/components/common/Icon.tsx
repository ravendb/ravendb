import React, { HTMLAttributes } from "react";
import classNames from "classnames";

interface IconProps extends HTMLAttributes<HTMLDivElement> {
    icon: string;
    addon?: string;
    color?: string;
    className?: string;
}

export function Icon(props: IconProps) {
    const { icon, addon, color, className, ...rest } = props;
    const iconClasses = "icon-" + icon;
    const addonClasses = addon ? "icon-addon-" + addon : null;
    const colorClasses = color ? "text-" + color : null;
    return <i className={classNames(iconClasses, addonClasses, colorClasses, className)} {...rest} />;
}
