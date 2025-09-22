import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { BackupsPage } from "./BackupsPage";
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Tasks/Backups",
    component: BackupsPage,
    decorators: [withStorybookContexts, withForceRerender, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/MJ7dtcrwfaTbzxZ9KdETUS/Pages---Backups?node-id=0-1&t=GpTL2Q8MFkRYsMfj-1",
        },
    },
} satisfies Meta<typeof BackupsPage>;

function commonInit() {
    const { accessManager, databases } = mockStore;

    databases.withActiveDatabase_Sharded();
    accessManager.with_securityClearance("ClusterAdmin");

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
}

interface BackupProps {
    disabled: boolean;
    encrypted: boolean;
    empty: boolean;
}

export const Default: StoryObj<BackupProps> = {
    render: (args) => {
        commonInit();

        const { tasksService } = mockServices;
        if (args.empty) {
            tasksService.withGetTasks((dto) => {
                dto.SubscriptionsCount = 0;
                dto.OngoingTasks = [];
                dto.PullReplications = [];
            });
            tasksService.withGetEtlProgress((dto) => {
                dto.Results = [];
            });

            tasksService.withGetManualBackup((x) => (x.Status = null));
        } else {
            tasksService.withGetTasks((x) => {
                const ongoingTask = TasksStubs.getPeriodicBackupListItem();
                if (args.disabled) {
                    ongoingTask.TaskState = "Disabled";
                }
                if (args.encrypted) {
                    ongoingTask.IsEncrypted = true;
                }
                x.OngoingTasks = [ongoingTask];
                x.PullReplications = [];
                x.SubscriptionsCount = 0;
            });
            tasksService.withGetEtlProgress();
            tasksService.withGetManualBackup();
        }

        return <BackupsPage />;
    },
    args: {
        disabled: false,
        empty: false,
        encrypted: false,
    },
};

export const FullView: StoryObj<BackupProps> = {
    ...Default,
    args: {
        ...Default.args,
        empty: false,
    },
};

export const EmptyView: StoryObj<BackupProps> = {
    ...Default,
    args: {
        ...Default.args,
        empty: true,
    },
};

export const Disabled: StoryObj<BackupProps> = {
    ...Default,
    args: {
        ...Default.args,
        disabled: true,
    },
};

export const Encrypted: StoryObj<BackupProps> = {
    ...Default,
    args: {
        ...Default.args,
        encrypted: true,
    },
};
