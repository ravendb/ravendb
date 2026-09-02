import { Meta, StoryObj } from "@storybook/react-webpack5";
import React from "react";
import moment from "moment";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import TimeSeriesFilterBadge from "./TimeSeriesFilterBadge";

export default {
    title: "Pages/Database/Time Series/Filter Badge",
    component: TimeSeriesFilterBadge,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof TimeSeriesFilterBadge>;

const noop = () => {
    // no-op for stories
};

export const Between: StoryObj<typeof TimeSeriesFilterBadge> = {
    render: () => (
        <TimeSeriesFilterBadge
            startDate={moment().subtract(30, "days")}
            endDate={moment()}
            onEdit={noop}
            onClear={noop}
        />
    ),
};

export const Before: StoryObj<typeof TimeSeriesFilterBadge> = {
    render: () => <TimeSeriesFilterBadge startDate={null} endDate={moment()} onEdit={noop} onClear={noop} />,
};

export const After: StoryObj<typeof TimeSeriesFilterBadge> = {
    render: () => (
        <TimeSeriesFilterBadge startDate={moment().subtract(7, "days")} endDate={null} onEdit={noop} onClear={noop} />
    ),
};
