import React, { ReactNode } from "react";
import appUrl from "common/appUrl";
import { Icon } from "components/common/Icon";
import { ThemeColor } from "components/models/common";
import IconName from "typings/server/icons";

interface ConsequenceProps {
    icon: IconName;
    iconColor?: ThemeColor;
    children: ReactNode;
}

export function Consequence({ icon, iconColor, children }: ConsequenceProps) {
    return (
        <li className="hstack gap-2 align-items-start">
            <span className="d-flex align-items-center" style={{ height: "1lh" }}>
                <Icon icon={icon} color={iconColor} margin="m-0" />
            </span>
            {children}
        </li>
    );
}

interface CopyDataHintProps {
    databaseName: string;
    onNavigate: () => void;
}

export function CopyDataHint({ databaseName, onNavigate }: CopyDataHintProps) {
    return (
        <p className="small text-muted mb-0">
            Need a copy of the data?{" "}
            <a href={appUrl.forBackups(databaseName)} onClick={onNavigate}>
                Back up
            </a>{" "}
            or{" "}
            <a href={appUrl.forExportDatabase(databaseName)} onClick={onNavigate}>
                export
            </a>{" "}
            first.
        </p>
    );
}
