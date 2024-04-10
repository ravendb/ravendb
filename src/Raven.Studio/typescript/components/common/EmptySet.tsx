import React, { ReactNode } from "react";
import { Icon, IconSize } from "./Icon";
import IconName from "typings/server/icons";
import classNames from "classnames";
import { TextColor } from "components/models/common";

interface EmptySetProps {
    children?: ReactNode | ReactNode[];
    icon?: IconName;
    color?: TextColor;
    className?: string;
    iconClassName?: string;
    iconSize?: IconSize;
}

export function EmptySet(props: EmptySetProps) {
    const { children, icon, color, className, iconSize = "xl" } = props;
    const defaultIcon: IconName = "empty-set";

    return (
        <div className={classNames("empty-set text-center mb-2 mx-auto", className)}>
            <Icon icon={icon || defaultIcon} color={color} margin="m-0" size={iconSize} />
            <div className="lead">{children}</div>
        </div>
    );
}
