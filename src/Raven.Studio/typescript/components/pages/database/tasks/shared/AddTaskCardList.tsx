import React, { ReactNode } from "react";
import "./AddTaskCardList.scss";
import Card from "react-bootstrap/Card";
import classNames from "classnames";
import { HrHeader } from "components/common/HrHeader";
import { Icon } from "components/common/Icon";
import { EmptySet } from "components/common/EmptySet";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useEventsCollector } from "hooks/useEventsCollector";
import LicenseRestrictedBadge, { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import IconName from "typings/server/icons";

export type TaskCardVariant = "AI" | "Replication" | "Backups" | "Subscriptions" | "ETL" | "Sink";

export interface TaskCardDisabledCondition {
    isActive: boolean;
    message: ReactNode;
}

export interface TaskCardInfo {
    title: string;
    description: string;
    iconName: IconName;
    variant: TaskCardVariant;
    link: string;
    target: string;
    licenseBadge?: LicenseBadgeText;
    showLicenseBadge?: boolean;
    counterBadge?: ReactNode;
    disabledConditions?: TaskCardDisabledCondition[];
}

export interface TaskCardCategory {
    categoryName: string;
    categoryIcon: IconName;
    tasks: TaskCardInfo[];
}

interface AddTaskCardListProps {
    categories: TaskCardCategory[];
    isAiOnly?: boolean;
}

export function AddTaskCardList({ categories, isAiOnly }: AddTaskCardListProps) {
    if (categories.length === 0) {
        return <EmptySet>No tasks match your filter criteria</EmptySet>;
    }

    return (
        <>
            {categories.map((category) => (
                <div className="pb-2 add-task-card-category" key={category.categoryName}>
                    {!isAiOnly && (
                        <HrHeader>
                            <Icon icon={category.categoryIcon} />
                            {category.categoryName}
                        </HrHeader>
                    )}
                    <div className="d-grid gap-3 add-task-card-grid">
                        {category.tasks.map((task) => (
                            <TaskCard key={task.title} {...task} />
                        ))}
                    </div>
                </div>
            ))}
        </>
    );
}

export function TaskCard({
    title,
    description,
    link,
    iconName,
    target,
    variant,
    licenseBadge,
    showLicenseBadge,
    counterBadge,
    disabledConditions,
}: TaskCardInfo) {
    const { reportEvent } = useEventsCollector();

    const conditions = disabledConditions ?? [];
    const isDisabled = conditions.some((x) => x.isActive);

    return (
        <ConditionalPopover className="w-100 h-100" conditions={conditions}>
            <a
                href={isDisabled ? undefined : link}
                onClick={() => !isDisabled && reportEvent(target, "new")}
                className={classNames("card no-decor w-100 h-100 add-task-card", `variant-${variant}`, {
                    "item-disabled": isDisabled,
                })}
            >
                <Card.Body className="d-flex flex-column gap-1">
                    <div className="d-flex align-items-center gap-2">
                        <Icon icon={iconName} className="task-icon" margin="m-0" />
                        <h4 className="mb-0">{title}</h4>
                        {counterBadge}
                    </div>
                    <div className="small">{description}</div>
                </Card.Body>

                {showLicenseBadge && (
                    <LicenseRestrictedBadge
                        className="position-absolute top-0 end-0 m-2"
                        licenseRequired={licenseBadge}
                    />
                )}
            </a>
        </ConditionalPopover>
    );
}
