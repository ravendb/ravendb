import React from "react";
import Button from "react-bootstrap/Button";
import appUrl from "common/appUrl";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { AddTaskCardList, TaskCardCategory } from "components/pages/database/tasks/shared/AddTaskCardList";
import { PerDatabaseOngoingTasksLink } from "./partials/PerDatabaseOngoingTasksLink";
import { ServerWideTasksInfoHub } from "./partials/ServerWideTasksInfoHub";

export default function AddServerWideTask() {
    const hasServerWideBackups = useAppSelector(licenseSelectors.statusValue("HasServerWideBackups"));
    const hasServerWideExternalReplications = useAppSelector(
        licenseSelectors.statusValue("HasServerWideExternalReplications")
    );

    const categories: TaskCardCategory[] = [
        {
            categoryName: "External replication",
            categoryIcon: "external-replication",
            tasks: [
                {
                    title: "Server-wide External Replication",
                    description:
                        "Create live replicas of all databases in your cluster, replicating each to a corresponding RavenDB database with the same name in another cluster.",
                    iconName: "external-replication",
                    variant: "Replication",
                    target: "serverWideExternalReplication",
                    link: appUrl.forEditServerWideExternalReplication(),
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasServerWideExternalReplications,
                },
            ],
        },
        {
            categoryName: "Backup",
            categoryIcon: "backup",
            tasks: [
                {
                    title: "Server-wide Backup",
                    description:
                        "Create periodic backups or snapshots of all databases in your cluster, managing schedule, retention, and destination from a single task.",
                    iconName: "periodic-backup",
                    variant: "Backups",
                    target: "serverWidePeriodicBackup",
                    link: appUrl.forEditServerWideBackup(),
                    licenseBadge: "Professional +",
                    showLicenseBadge: !hasServerWideBackups,
                },
            ],
        },
    ];

    return (
        <div className="content-margin">
            <div className="d-flex justify-content-between align-items-center mb-4">
                <AboutViewHeading
                    title="Add a Server-Wide Task"
                    icon="server-wide-tasks"
                    iconAddon="plus"
                    marginBottom={0}
                />
                <div className="d-flex align-items-center gap-3">
                    <PerDatabaseOngoingTasksLink />
                    <ServerWideTasksInfoHub />
                </div>
            </div>
            <Button href={appUrl.forServerWideTasks()} className="rounded-pill mb-3" variant="secondary">
                <Icon icon="arrow-left" />
                Back to server-wide tasks
            </Button>
            <AddTaskCardList categories={categories} />
        </div>
    );
}
