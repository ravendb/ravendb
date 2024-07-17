import React from "react";
import { Meta, StoryFn } from "@storybook/react";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { boundCopy } from "components/utils/common";
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
import { BackupsPage } from "./BackupsPage";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Backups",
    component: BackupsPage,
    decorators: [withStorybookContexts, withForceRerender, withBootstrap5],
    excludeStories: /Template$/,
} satisfies Meta<typeof BackupsPage>;

function commonInit() {
    const { accessManager, databases } = mockStore;

    databases.withActiveDatabase_Sharded();
    accessManager.with_securityClearance("ClusterAdmin");

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
}

export const EmptyView: StoryFn<typeof BackupsPage> = () => {
    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((dto) => {
        dto.SubscriptionsCount = 0;
        dto.OngoingTasks = [];
        dto.PullReplications = [];
    });
    tasksService.withGetProgress((dto) => {
        dto.Results = [];
    });

    tasksService.withGetManualBackup((x) => (x.Status = null));

    return <BackupsPage />;
};

export const FullView: StoryFn<typeof BackupsPage> = () => {
    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks();
    tasksService.withGetProgress();
    tasksService.withGetManualBackup();

    return <BackupsPage />;
};

export const PeriodicBackupTemplate = (args: {
    disabled?: boolean;
    customizeTask?: (x: OngoingTaskBackup) => void;
}) => {
    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const ongoingTask = TasksStubs.getPeriodicBackupListItem();
        if (args.disabled) {
            ongoingTask.TaskState = "Disabled";
        }
        args.customizeTask?.(ongoingTask);
        x.OngoingTasks = [ongoingTask];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });

    tasksService.withGetManualBackup();

    return <BackupsPage />;
};

export const PeriodicBackupDisabled = boundCopy(PeriodicBackupTemplate, {
    disabled: true,
});

export const PeriodicBackupEnabledEncrypted = boundCopy(PeriodicBackupTemplate, {
    disabled: false,
    customizeTask: (x) => (x.IsEncrypted = true),
});
