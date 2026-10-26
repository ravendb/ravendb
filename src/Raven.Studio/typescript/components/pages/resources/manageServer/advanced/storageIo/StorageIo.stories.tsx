import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import StorageIo from "./StorageIo";
import { mockServices } from "test/mocks/services/MockServices";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";

export default {
    title: "Pages/Manage Server/Advanced/Storage I/O",
    component: StorageIo,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof StorageIo>;

// node B of RavenDB-27821: disk saturated, writes paced, one index left the shared journal
export const Saturated: StoryObj<typeof StorageIo> = {
    render: () => {
        mockServices.manageServerService.withGetStorageIo();
        return <StorageIo />;
    },
};

export const Healthy: StoryObj<typeof StorageIo> = {
    render: () => {
        const report = ManageServerStubs.storageIo();
        const device = report.Devices[0];
        device.Status = "Ok";
        device.StatusReason = "The disk keeps up with the writes.";
        device.Draining = false;
        device.DrainShareOfLastHour = 0;
        device.JournalWriteLatencyInMs = 0.9;
        device.SyncCostInMs = 8;
        device.QueueDepth = 0.4;
        device.SaturatedSince = null;
        device.SlowWrites = [];
        device.History = device.History.map((s) => ({
            ...s,
            Draining: false,
            JournalWriteLatencyInMs: 0.9,
            QueueDepth: 0.4,
        }));
        report.SharedJournals[0].IndexesNotSharing = [];
        report.Environments.forEach(
            (e) => (e.Sync = { ...e.Sync, LastReason: "Idle", LastDrain: false, LastWritebackInMs: 0 })
        );

        mockServices.manageServerService.withGetStorageIo(report);
        return <StorageIo />;
    },
};
