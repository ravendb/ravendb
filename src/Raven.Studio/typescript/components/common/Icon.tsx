import React from "react";
import classNames from "classnames";

interface IconProps {
    icon: string;
    addon?: string;
    className?: string;
}

export function Icon(props: IconProps) {
    const { icon, addon, className } = props;
    const iconClasses = "icon-" + icon;
    const addonClasses = addon ? "icon-addon-" + addon : null;
    return (
        <>
            <i className={classNames(iconClasses, addonClasses, className)} />
        </>
    );
}
