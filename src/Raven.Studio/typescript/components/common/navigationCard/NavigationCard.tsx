import "./NavigationCard.scss";
import Card from "react-bootstrap/Card";
import React, { ReactNode } from "react";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import classNames from "classnames";
import { useEventsCollector } from "hooks/useEventsCollector";
import LicenseRestrictedBadge, { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { AccessPopover } from "components/common/AccessPopover";

export type NavigationCardVariant =
    | "AI"
    | "Replication"
    | "Backups"
    | "Subscriptions"
    | "ETL"
    | "Sink"
    | "ImportFile"
    | "ImportRavenDb"
    | "ImportCsv"
    | "ImportSql"
    | "ImportNoSql";

export interface NavigationCardProps {
    title: string;
    description: string;
    iconName: IconName;
    variant: NavigationCardVariant;
    link: string;
    target: string;
    licenseBadge?: LicenseBadgeText;
    counterBadge?: ReactNode;
    showLicenseBadge?: boolean;
    isShardingSupported?: boolean;
    accessRequired: databaseAccessLevel;
    customDisabledReason?: ReactNode;
}

export default function NavigationCard({
    title,
    description,
    link,
    iconName,
    target,
    variant,
    licenseBadge,
    showLicenseBadge,
    counterBadge,
    isShardingSupported,
    accessRequired,
    customDisabledReason,
}: NavigationCardProps) {
    const { reportEvent } = useEventsCollector();
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;
    const canHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation)(accessRequired);

    const isShardingNotSupported = !isShardingSupported && isSharded;
    const isDisabled = isShardingNotSupported || !canHandleOperation || !!customDisabledReason;

    return (
        <AccessPopover
            className="w-100 h-100"
            accessRequired={accessRequired}
            conditions={[
                {
                    isActive: isShardingNotSupported,
                    message: "Sharding is not supported for this task",
                },
                {
                    isActive: !!customDisabledReason,
                    message: customDisabledReason,
                },
            ]}
        >
            <a
                href={isDisabled ? undefined : link}
                onClick={() => reportEvent(target, "new")}
                className={classNames("card no-decor w-100 h-100 navigation-card", `variant-${variant}`, {
                    "item-disabled": !!isDisabled,
                })}
            >
                <Card.Body className="d-flex align-items gap-3">
                    <div className="align-self-center">
                        <Icon icon={iconName} className="task-icon fs-2" />
                    </div>
                    <div className="d-flex flex-column align-self-center gap-1">
                        <div className="d-flex align-items-center gap-2">
                            <h4 className="mb-0">{title}</h4>
                            {counterBadge}
                        </div>
                        <div>{description}</div>
                    </div>
                </Card.Body>

                {showLicenseBadge && (
                    <LicenseRestrictedBadge
                        className="position-absolute top-0 end-0 m-2"
                        licenseRequired={licenseBadge}
                    />
                )}
            </a>
        </AccessPopover>
    );
}
