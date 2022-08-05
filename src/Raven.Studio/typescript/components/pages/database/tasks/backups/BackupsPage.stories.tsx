import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { forceStoryRerender, withStorybookContexts } from "../../../../../test/storybookTestUtils";
import { DatabasesStubs } from "../../../../../test/stubs/DatabasesStubs";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { mockServices } from "../../../../../test/mocks/MockServices";
import { TasksStubs } from "../../../../../test/stubs/TasksStubs";
import { boundCopy } from "../../../../utils/common";
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
import { BackupsPage } from "./BackupsPage";

function tasksHolder(storyFn: any) {
    return (
        <div
            className="flex-vertical absolute-fill content-margin backups"
            style={{ height: "100vh", overflow: "auto" }}
        >
            {storyFn()}
        </div>
    );
}

export default {
    title: "Pages/Backups",
    component: BackupsPage,
    decorators: [withStorybookContexts, tasksHolder],
    excludeStories: /Template$/,
} as ComponentMeta<typeof BackupsPage>;

function commonInit() {
    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
}

export const EmptyView: ComponentStory<typeof BackupsPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((dto) => {
        dto.SubscriptionsCount = 0;
        dto.OngoingTasksList = [];
        dto.PullReplications = [];
    });
    tasksService.withGetProgress((dto) => {
        dto.Results = [];
    });

    tasksService.withGetManualBackup((x) => (x.Status = null));

    return <BackupsPage database={db} />;
};

export const FullView: ComponentStory<typeof BackupsPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks();
    tasksService.withGetProgress();
    tasksService.withGetManualBackup();

    return <BackupsPage database={db} />;
};

export const PeriodicBackupTemplate = (args: {
    disabled?: boolean;
    customizeTask?: (x: OngoingTaskBackup) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const ongoingTask = TasksStubs.getPeriodicBackupListItem();
        if (args.disabled) {
            ongoingTask.TaskState = "Disabled";
        }
        args.customizeTask?.(ongoingTask);
        x.OngoingTasksList = [ongoingTask];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });

    tasksService.withGetManualBackup();

    return <BackupsPage {...forceStoryRerender()} database={db} />;
};

export const PeriodicBackupDisabled = boundCopy(PeriodicBackupTemplate, {
    disabled: true,
});

export const PeriodicBackupEnabledEncrypted = boundCopy(PeriodicBackupTemplate, {
    disabled: false,
    customizeTask: (x) => (x.IsEncrypted = true),
});
