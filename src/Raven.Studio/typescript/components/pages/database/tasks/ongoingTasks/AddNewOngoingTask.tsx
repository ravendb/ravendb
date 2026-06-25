import { HrHeader } from "components/common/HrHeader";
import "./AddNewOngoingTask.scss";
import { AboutViewHeading } from "components/common/AboutView";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import React, { ReactNode, useEffect, useRef, useState } from "react";
import { Icon } from "components/common/Icon";
import IconName from "typings/server/icons";
import classNames from "classnames";
import { useAppUrls } from "hooks/useAppUrls";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useEventsCollector } from "hooks/useEventsCollector";
import LicenseRestrictedBadge, { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { useNewOngoingTasks } from "components/pages/database/tasks/shared/shared";
import { EmptySet } from "components/common/EmptySet";
import { AddNewOngoingTaskAboutView } from "components/pages/database/tasks/ongoingTasks/partials/AddNewOngoingTaskAboutView";
import { RadioToggleWithIcon } from "components/common/toggles/RadioToggle";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { getDatabaseAccessRequiredMessage } from "components/utils/accessUtils";

interface AddNewOngoingTaskProps {
    queryParams?: { noBack?: string };
}

const getCategoryId = (categoryName: string) =>
    `ongoing-task-category-${categoryName.replace(/[^a-zA-Z0-9]/g, "-").toLowerCase()}`;

export default function AddNewOngoingTask({ queryParams }: AddNewOngoingTaskProps) {
    const { forCurrentDatabase, appUrl } = useAppUrls();
    const { filteredTasks, allCategories, searchText, setSearchText } = useNewOngoingTasks();

    const serverWideTasksUrl = appUrl.forServerWideTasks();
    const ongoingTasksUrl = forCurrentDatabase.ongoingTasksUrl();
    const showBackUrl = !queryParams?.noBack;

    const [activeCategory, setActiveCategory] = useState<string | null>(
        allCategories.length > 0 ? allCategories[0].categoryName : null
    );
    const [displayMode, setDisplayMode] = useState<"expanded" | "compact">("expanded");

    const contentRef = useRef<HTMLDivElement>(null);
    const isScrollingRef = useRef(false);
    const scrollTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

    const scrollToCategory = (categoryName: string) => {
        const el = document.getElementById(getCategoryId(categoryName));
        if (el) {
            isScrollingRef.current = true;
            el.scrollIntoView({ behavior: "smooth", block: "start" });
        }
        setActiveCategory(categoryName);
    };

    useEffect(() => {
        const container = contentRef.current;
        if (!container || allCategories.length === 0) {
            return;
        }

        const handleScroll = () => {
            if (isScrollingRef.current) {
                // Debounce: clear the flag 150ms after the last scroll tick from the animation
                clearTimeout(scrollTimerRef.current);
                scrollTimerRef.current = setTimeout(() => {
                    isScrollingRef.current = false;
                }, 150);
                return;
            }

            const maxScrollTop = container.scrollHeight - container.clientHeight;
            const scrollProgress = maxScrollTop > 0 ? container.scrollTop / maxScrollTop : 0;
            const triggerFraction = 0.15 + scrollProgress * 0.7;
            const containerTop = container.getBoundingClientRect().top;
            const triggerY = containerTop + container.clientHeight * triggerFraction;

            let active = allCategories[0].categoryName;
            for (const category of allCategories) {
                const el = document.getElementById(getCategoryId(category.categoryName));
                if (el && el.getBoundingClientRect().top <= triggerY) {
                    active = category.categoryName;
                }
            }

            setActiveCategory(active);
        };

        container.addEventListener("scroll", handleScroll, { passive: true });

        return () => {
            container.removeEventListener("scroll", handleScroll);
            clearTimeout(scrollTimerRef.current);
        };
    }, [allCategories]);

    return (
        <div className="add-new-ongoing-task d-flex flex-column">
            <div className="d-flex justify-content-between align-items-start">
                {showBackUrl ? (
                    <AboutViewHeading
                        title="Add a database task"
                        icon="tasks"
                        iconAddon="plus"
                        backUrl={ongoingTasksUrl}
                        marginBottom={4}
                    />
                ) : (
                    <AboutViewHeading title="Add a database task" icon="tasks" iconAddon="plus" marginBottom={4} />
                )}
                <div className="d-flex align-items-center gap-3">
                    <RadioToggleWithIcon
                        name="task-display-mode"
                        leftItem={{ label: "", value: "expanded", iconName: "list" }}
                        rightItem={{ label: "", value: "compact", iconName: "grid-3x2" }}
                        selectedValue={displayMode}
                        setSelectedValue={(val) => setDisplayMode(val)}
                    />
                    <AddNewOngoingTaskAboutView />
                </div>
            </div>
            <div className="add-new-ongoing-task-horizontal-nav gap-1">
                <div>
                    <div className="small-label ms-1 mb-1">Search by name</div>
                    <div className="clearable-input mb-2">
                        <Form.Control
                            type="text"
                            accessKey="/"
                            placeholder="e.g. External Replication"
                            title="Filter tasks"
                            className="filtering-input"
                            value={searchText}
                            onChange={(e) => setSearchText(e.target.value)}
                        />
                        {searchText && (
                            <div className="clear-button">
                                <Button variant="secondary" size="sm" onClick={() => setSearchText("")}>
                                    <Icon icon="clear" margin="m-0" />
                                </Button>
                            </div>
                        )}
                    </div>
                </div>
                <div>
                    <div className="small-label ms-1 mb-1">Task Categories</div>
                    <div className="add-new-ongoing-task-chips-row">
                        {allCategories.map((category) => {
                            const isAvailable = filteredTasks.some((t) => t.categoryName === category.categoryName);
                            return (
                                <button
                                    key={category.categoryName}
                                    className={classNames("add-new-ongoing-task-chip", {
                                        active: activeCategory === category.categoryName,
                                    })}
                                    onClick={() => scrollToCategory(category.categoryName)}
                                    disabled={!isAvailable}
                                >
                                    <Icon icon={category.categoryIcon} margin="m-0" />
                                    <span>{category.categoryName}</span>
                                </button>
                            );
                        })}
                    </div>
                </div>
            </div>
            <div className="add-new-ongoing-task-layout d-flex gap-4 mt-2">
                <div className="add-new-ongoing-task-sidebar flex-shrink-0 p-3">
                    <div className="small-label ms-1 mb-1">Search by name</div>
                    <div className="clearable-input mb-3">
                        <Form.Control
                            type="text"
                            accessKey="/"
                            placeholder="e.g. External Replication"
                            title="Filter tasks"
                            className="filtering-input"
                            value={searchText}
                            onChange={(e) => setSearchText(e.target.value)}
                        />
                        {searchText && (
                            <div className="clear-button">
                                <Button variant="secondary" size="sm" onClick={() => setSearchText("")}>
                                    <Icon icon="clear" margin="m-0" />
                                </Button>
                            </div>
                        )}
                    </div>
                    {displayMode === "expanded" && (
                        <>
                            <div className="small-label ms-1 mb-1">Task Categories</div>
                            <div className="d-flex flex-column">
                                {allCategories.map((category) => {
                                    const isAvailable = filteredTasks.some(
                                        (t) => t.categoryName === category.categoryName
                                    );
                                    return (
                                        <button
                                            key={category.categoryName}
                                            className={classNames("add-new-ongoing-task-nav-item", {
                                                active: activeCategory === category.categoryName,
                                                disabled: !isAvailable,
                                            })}
                                            onClick={() => scrollToCategory(category.categoryName)}
                                            disabled={!isAvailable}
                                        >
                                            <Icon icon={category.categoryIcon} margin="m-0" />
                                            <span>{category.categoryName}</span>
                                        </button>
                                    );
                                })}
                            </div>
                        </>
                    )}
                    <hr className="my-3" />
                    <div className="small ms-1 text-muted">Need a cluster-wide task? Check out:</div>
                    <a
                        href={serverWideTasksUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="add-new-ongoing-task-nav-item text-decoration-none"
                    >
                        <Icon icon="server-wide-tasks" margin="m-0" />
                        <span>Server-Wide Tasks</span>
                        <Icon icon="newtab" margin="ms-0 m-0" className="add-new-ongoing-task-nav-item__newtab" />
                    </a>
                </div>
                <div ref={contentRef} className="add-new-ongoing-task-content pb-4">
                    <OngoingTasksList
                        filteredTasks={filteredTasks}
                        getCategoryId={getCategoryId}
                        displayMode={displayMode}
                    />
                </div>
            </div>
        </div>
    );
}

interface TaskCategory {
    categoryName: string;
    categoryHeaderName?: string;
    categoryIcon: IconName;
    tasks: TaskItemProps[];
}

interface OngoingTasksListProps {
    filteredTasks: TaskCategory[];
    getCategoryId?: (categoryName: string) => string;
    displayMode?: "expanded" | "compact";
}

export function OngoingTasksList({ filteredTasks, getCategoryId, displayMode = "expanded" }: OngoingTasksListProps) {
    if (filteredTasks.length === 0) {
        return <EmptySet>No tasks match your filter criteria</EmptySet>;
    }

    const isCompact = displayMode === "compact";

    return (
        <>
            {filteredTasks.map((category, index) => (
                <div className="pb-2" key={index} id={getCategoryId ? getCategoryId(category.categoryName) : undefined}>
                    <HrHeader>
                        <Icon icon={category.categoryIcon} />
                        {category.categoryHeaderName ?? category.categoryName}
                    </HrHeader>
                    <div
                        className={classNames(
                            "d-grid ongoing-tasks-grid",
                            isCompact ? "gap-2 ongoing-tasks-grid--compact" : "gap-3"
                        )}
                    >
                        {category.tasks.map((task) => (
                            <TaskItem key={task.title} {...task} displayMode={displayMode} />
                        ))}
                    </div>
                </div>
            ))}
        </>
    );
}

type TaskCardVariant = "Replication" | "Backups" | "Subscriptions" | "ETL" | "Sink";

export interface TaskItemProps {
    title: string;
    description: string;
    iconName: IconName;
    variant: TaskCardVariant;
    link: string;
    target: string;
    licenseBadge?: LicenseBadgeText;
    counterBadge?: ReactNode;
    showLicenseBadge?: boolean;
    isShardingSupported?: boolean;
    accessRequired: databaseAccessLevel;
    customDisabledReason?: ReactNode;
    displayMode?: "expanded" | "compact";
}

function TaskItem({
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
    displayMode = "expanded",
}: TaskItemProps) {
    const { reportEvent } = useEventsCollector();
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;
    const canHandleOperation = useAppSelector(accessManagerSelectors.getCanHandleOperation)(accessRequired);

    const isShardingNotSupported = !isShardingSupported && isSharded;
    const isDisabled = isShardingNotSupported || !canHandleOperation || !!customDisabledReason;

    const isCompact = displayMode === "compact";

    return (
        <ConditionalPopover
            className="w-100 h-100"
            conditions={[
                {
                    isActive: !canHandleOperation,
                    message: getDatabaseAccessRequiredMessage(accessRequired),
                },
                {
                    isActive: !!isShardingNotSupported,
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
                className={classNames(
                    "card no-decor w-100 ongoing-tasks-card h-100 add-new-ongoing-task-card",
                    `variant-${variant}`,
                    {
                        "item-disabled": !!isDisabled,
                        compact: isCompact,
                    }
                )}
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
