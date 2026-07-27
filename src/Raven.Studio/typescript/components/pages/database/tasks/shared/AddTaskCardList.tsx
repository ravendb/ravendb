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

export type TaskCardDisplayMode = "expanded" | "compact";

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
    categoryHeaderName?: string;
    categoryIcon: IconName;
    tasks: TaskCardInfo[];
}

interface AddTaskCardListProps {
    categories: TaskCardCategory[];
    isAiOnly?: boolean;
    displayMode?: TaskCardDisplayMode;
}

export function AddTaskCardList({ categories, isAiOnly, displayMode = "expanded" }: AddTaskCardListProps) {
    if (categories.length === 0) {
        return <EmptySet>No tasks match your filter criteria</EmptySet>;
    }

    const isCompact = displayMode === "compact";

    return (
        <>
            {categories.map((category) => (
                <div className="pb-2 add-task-card-category" key={category.categoryName}>
                    {!isAiOnly && (
                        <HrHeader>
                            <Icon icon={category.categoryIcon} />
                            {category.categoryHeaderName ?? category.categoryName}
                        </HrHeader>
                    )}
                    <div
                        className={classNames(
                            "d-grid add-task-card-grid",
                            isCompact ? "gap-2 add-task-card-grid--compact" : "gap-3"
                        )}
                    >
                        {category.tasks.map((task) => (
                            <TaskCard key={task.title} {...task} displayMode={displayMode} />
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
    displayMode = "expanded",
}: TaskCardInfo & { displayMode?: TaskCardDisplayMode }) {
    const { reportEvent } = useEventsCollector();

    const conditions = disabledConditions ?? [];
    const isDisabled = conditions.some((x) => x.isActive);
    const isCompact = displayMode === "compact";

    return (
        <ConditionalPopover className="w-100 h-100" conditions={conditions}>
            <a
                href={isDisabled ? undefined : link}
                onClick={() => !isDisabled && reportEvent(target, "new")}
                className={classNames("card no-decor w-100 h-100 add-task-card", `variant-${variant}`, {
                    "item-disabled": isDisabled,
                    compact: isCompact,
                })}
            >
                <Card.Body className={isCompact ? "d-flex align-items-center" : "d-flex flex-column gap-1"}>
                    <div className="d-flex align-items-center">
                        <Icon icon={iconName} className="task-icon" margin="me-2" />
                        <h4 className="mb-0">{title}</h4>
                        {counterBadge}
                    </div>
                    {!isCompact && <div className="small">{description}</div>}
                </Card.Body>

                {showLicenseBadge && licenseBadge && (
                    <LicenseRestrictedBadge
                        className="position-absolute top-0 end-0 m-2"
                        licenseRequired={licenseBadge}
                    />
                )}
            </a>
        </ConditionalPopover>
    );
}
