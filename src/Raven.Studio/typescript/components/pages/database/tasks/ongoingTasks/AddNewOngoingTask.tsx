import { HrHeader } from "components/common/HrHeader";
import "./AddNewOngoingTask.scss";
import { AboutViewHeading } from "components/common/AboutView";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Col from "react-bootstrap/Col";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import React, { ReactNode } from "react";
import { Icon } from "components/common/Icon";
import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import IconName from "typings/server/icons";
import classNames from "classnames";
import { useAppUrls } from "hooks/useAppUrls";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { useEventsCollector } from "hooks/useEventsCollector";
import LicenseRestrictedBadge, { LicenseBadgeText } from "components/common/LicenseRestrictedBadge";
import { useNewOngoingTasks } from "components/pages/database/tasks/shared/shared";
import { EmptySet } from "components/common/EmptySet";
import { AddNewOngoingTaskAboutView } from "components/pages/database/tasks/ongoingTasks/partials/AddNewOngoingTaskAboutView";

export default function AddNewOngoingTask() {
    const { forCurrentDatabase, appUrl } = useAppUrls();
    const { filteredTasks, categoryList, searchText, selectedCategories, setSearchText, setSelectedCategories } =
        useNewOngoingTasks();

    const serverWideTasksUrl = appUrl.forServerWideTasks();
    const ongoingTasksUrl = forCurrentDatabase.ongoingTasksUrl();

    return (
        <div className="content-margin add-new-ongoing-task">
            <div className="d-flex justify-content-between">
                <AboutViewHeading title="Add a database task" icon="tasks" iconAddon="plus" marginBottom={4} />
                <div className="d-flex align-items-start gap-3">
                    <Button
                        size="sm"
                        target="_blank"
                        href={serverWideTasksUrl}
                        title="Go to the Server-Wide Tasks view"
                        variant="link"
                    >
                        <Icon icon="server-wide-tasks" />
                        Server-Wide Tasks
                    </Button>
                    <AddNewOngoingTaskAboutView />
                </div>
            </div>
            <Button href={ongoingTasksUrl} className="rounded-pill" variant="secondary">
                <Icon icon="arrow-left" />
                Back to ongoing tasks
            </Button>
            <Row className="d-flex row-gap-2 mt-3">
                <Col className="mb-2">
                    <div className="flex-grow">
                        <div className="small-label ms-1 mb-1">Search by name</div>
                        <div className="clearable-input">
                            <Form.Control
                                type="text"
                                accessKey="/"
                                placeholder="e.g. Embeddings Generation"
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
                </Col>
                <Col xs="auto">
                    <MultiCheckboxToggle
                        inputItems={categoryList}
                        label="Filter by category"
                        selectedItems={selectedCategories}
                        setSelectedItems={(x) => setSelectedCategories(x)}
                        selectAll
                        selectAllLabel="Select All"
                    />
                </Col>
            </Row>
            {filteredTasks.length > 0 ? (
                filteredTasks.map((category, index) => (
                    <div className="pb-2" key={index}>
                        <HrHeader>
                            <Icon icon={category.categoryIcon} />
                            {category.categoryName}
                        </HrHeader>
                        <div className="d-grid gap-3 ongoing-tasks-grid">
                            {category.tasks.map((task, idx) => (
                                <div key={idx}>
                                    <TaskItem {...task} />
                                </div>
                            ))}
                        </div>
                    </div>
                ))
            ) : (
                <EmptySet>No tasks match your filter criteria</EmptySet>
            )}
        </div>
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
    disableReason?: ReactNode;
    licenseBadge?: LicenseBadgeText;
    showLicenseBadge?: boolean;

    counterBadge?: ReactNode;
}

function TaskItem({
    title,
    description,
    link,
    disableReason,
    iconName,
    target,
    variant,
    licenseBadge,
    showLicenseBadge,
    counterBadge,
}: TaskItemProps) {
    const { reportEvent } = useEventsCollector();
    return (
        <ConditionalPopover
            className="w-100 h-100"
            conditions={{
                isActive: !!disableReason,
                message: disableReason,
            }}
        >
            <a
                href={link}
                onClick={() => reportEvent(target, "new")}
                className={classNames(
                    "card no-decor w-100 ongoing-tasks-card h-100 add-new-ongoing-task__card",
                    `variant-${variant}`,
                    {
                        "opacity-25 pe-none": !!disableReason,
                    }
                )}
            >
                <Card.Body className="d-flex align-items gap-3">
                    <div className="align-self-center">
                        <Icon icon={iconName} className="task-icon fs-2" />
                    </div>
                    <div className="d-flex flex-column gap-1">
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
        </ConditionalPopover>
    );
}
