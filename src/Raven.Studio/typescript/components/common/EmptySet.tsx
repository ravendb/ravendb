import React, { ReactNode } from "react";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";
import classNames from "classnames";
import { TextColor } from "components/models/common";

interface EmptySetProps {
    children?: ReactNode | ReactNode[];
    icon?: IconName;
    color?: TextColor;
    className?: string;
}

export function EmptySet(props: EmptySetProps) {
    const { children, icon, color, className } = props;
    const defaultIcon: IconName = "empty-set";

    return (
        <div className={classNames("empty-set text-center mb-2 mx-auto", className)}>
            <Icon icon={icon || defaultIcon} color={color} className="icon-xl" margin="m-0" />
            <div className="lead">{children}</div>
        </div>
    );
}
