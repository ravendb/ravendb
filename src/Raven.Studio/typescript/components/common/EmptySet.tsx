import React, { ReactNode } from "react";
import { Icon, IconSize } from "./Icon";
import IconName from "typings/server/icons";
import classNames from "classnames";
import { TextColor } from "components/models/common";
import "./EmptySet.scss";

interface EmptySetProps {
    children?: ReactNode | ReactNode[];
    icon?: IconName;
    color?: TextColor;
    className?: string;
    iconClassName?: string;
    iconSize?: IconSize;
    compact?: boolean;
}

export function EmptySet(props: EmptySetProps) {
    const { children, icon, color, className, compact, iconSize = "xl" } = props;
    const defaultIcon: IconName = "empty-set";

    return (
        <div className={classNames("empty-set", className, compact ? "compact my-2" : "mb-2 mx-auto text-center")}>
            <Icon icon={icon || defaultIcon} color={color} margin="m-0" size={compact ? "sm" : iconSize} />
            <div className={classNames("mb-0", compact ? "lh-1" : "lead")}>{children}</div>
        </div>
    );
}
