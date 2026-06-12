import React, { ReactNode, useLayoutEffect, useRef, useState } from "react";
import classNames from "classnames";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import { ThemeColor } from "components/models/common";
import { PopoverWithHover } from "components/common/PopoverWithHover";
import Popover from "react-bootstrap/Popover";

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
    const spanRef = useRef<HTMLSpanElement>(null);
    const [isOverflowing, setIsOverflowing] = useState(false);

    useLayoutEffect(() => {
        const el = spanRef.current;
        if (el) {
            setIsOverflowing(el.scrollWidth > el.clientWidth);
        }
    }, [stringValue]);

    return (
        <>
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
                        <span ref={spanRef} className="stat-tile-text">
                            {stringValue}
                        </span>
                    ) : (
                        value
                    )}
                </div>
            </div>
            {isOverflowing && spanRef.current && (
                <PopoverWithHover target={spanRef.current} placement="top">
                    <Popover.Body>{stringValue}</Popover.Body>
                </PopoverWithHover>
            )}
        </>
    );
}
