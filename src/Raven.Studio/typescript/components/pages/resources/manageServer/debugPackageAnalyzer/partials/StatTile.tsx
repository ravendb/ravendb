import React, { ReactNode } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";

interface StatTileProps {
    label: string;
    icon: IconName;
    iconColor?: ThemeColor;
    value: ReactNode;
    valueColor?: ThemeColor;
}

// shared overview tile: small label on top, then an icon + value (matches the Figma cluster/node/storage tiles)
export default function StatTile({ label, icon, iconColor, value, valueColor }: StatTileProps) {
    return (
        <div className="stat-tile well px-3 py-2 rounded">
            <div className="stat-tile-label text-muted small">{label}</div>
            <div
                className={classNames(
                    "stat-tile-value hstack gap-1 align-items-center fs-5",
                    valueColor && `text-${valueColor}`
                )}
            >
                <Icon icon={icon} color={iconColor} margin="m-0" /> {value}
            </div>
        </div>
    );
}
