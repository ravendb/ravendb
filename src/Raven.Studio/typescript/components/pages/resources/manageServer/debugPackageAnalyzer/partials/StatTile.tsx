import React, { ReactNode } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import "./StatTile.scss";

interface StatTileProps {
    label: string;
    icon: IconName;
    iconColor?: ThemeColor;
    value: ReactNode;
    valueColor?: ThemeColor;
    valueClassName?: string;
    className?: string;
}

// shared overview tile: small label on top, then an icon + value (matches the Figma cluster/node/storage tiles)
export default function StatTile({
    label,
    icon,
    iconColor,
    value,
    valueColor,
    valueClassName,
    className,
}: StatTileProps) {
    const stringValue = typeof value === "string" ? value : undefined;

    return (
        <div className={classNames("stat-tile panel-bg-2 px-3 py-2 rounded", className)}>
            <div className="stat-tile-label text-muted small" title={label}>
                {label}
            </div>
            <div
                className={classNames(
                    "stat-tile-value hstack gap-1 align-items-center fs-5",
                    valueColor && `text-${valueColor}`,
                    valueClassName
                )}
            >
                <Icon icon={icon} color={iconColor} margin="m-0" />
                {stringValue !== undefined ? (
                    <PopoverWithHoverWrapper message={stringValue} placement="top" wrapperClassName="stat-tile-text">
                        {stringValue}
                    </PopoverWithHoverWrapper>
                ) : (
                    value
                )}
            </div>
        </div>
    );
}
