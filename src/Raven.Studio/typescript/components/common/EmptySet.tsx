import React, { ReactNode } from "react";
import { Icon } from "./Icon";
import IconName from "typings/server/icons";

interface EmptySetProps {
    children?: ReactNode | ReactNode[];
    icon?: IconName;
    color?: string | "muted";
}

export function EmptySet(props: EmptySetProps) {
    const { children, icon, color } = props;
    const defaultIcon: IconName = "empty-set";

    return (
        <div className="empty-set text-center mb-2 mx-auto">
            <Icon icon={icon || defaultIcon} color={color} className="icon-xl" margin="m-0" />
            <div className="lead">{children}</div>
        </div>
    );
}
