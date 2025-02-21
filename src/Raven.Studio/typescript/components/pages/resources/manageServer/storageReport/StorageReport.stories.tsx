import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import { mockServices } from "test/mocks/services/MockServices";
import React from "react";
import StorageReport from "components/pages/resources/manageServer/storageReport/StorageReport";
import { userEvent, within } from "@storybook/test";
import { delay } from "components/utils/common";

export default {
    title: "Pages/ManageServer/Storage Report",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

export const StorageReportStory: StoryObj = {
    name: "Storage Report",
    render: () => {
        const { manageServerService } = mockServices;

        manageServerService.withServerWideStorageReport();

        return <StorageReport />;
    },
};

const delayInMs = 500;

export const NestedData: StoryObj = {
    ...StorageReportStory,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const chart = await canvas.findByTestId("chart");

        const chartSelectors = within(chart);

        await delay(delayInMs);

        const datafile = await chartSelectors.findByText("Datafile");
        await userEvent.click(datafile.closest(".cell"));

        await delay(delayInMs);

        const tables = await chartSelectors.findByText("Tables");
        await userEvent.click(tables.closest(".cell"));

        await delay(delayInMs);

        const compareExchange = await chartSelectors.findByText("CompareExchange");
        await userEvent.click(compareExchange.closest(".cell"));
    },
};
