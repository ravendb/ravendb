import { Meta, StoryObj } from "@storybook/react-webpack5";
import React from "react";
import moment from "moment";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import DeleteTimeSeriesRangeModal, { TimeSeriesRangeCount } from "./DeleteTimeSeriesRangeModal";

export default {
    title: "Pages/Database/Time Series/Delete Time Series Range Modal",
    component: DeleteTimeSeriesRangeModal,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DeleteTimeSeriesRangeModal>;

const noop = () => {
    // no-op for stories
};

const resolve = () => Promise.resolve();

const countOf = (result: TimeSeriesRangeCount) => (): Promise<TimeSeriesRangeCount> => Promise.resolve(result);

export const ExactCount: StoryObj<typeof DeleteTimeSeriesRangeModal> = {
    render: () => (
        <DeleteTimeSeriesRangeModal
            timeSeriesName="HeartRate"
            startDate={moment().subtract(7, "days")}
            endDate={moment()}
            resolveCount={countOf({ count: 1284, exact: true })}
            onDelete={resolve}
            close={noop}
        />
    ),
};

export const LowerBoundCount: StoryObj<typeof DeleteTimeSeriesRangeModal> = {
    render: () => (
        <DeleteTimeSeriesRangeModal
            timeSeriesName="HeartRate"
            startDate={moment().subtract(7, "days")}
            endDate={moment()}
            resolveCount={countOf({ count: 100, exact: false })}
            onDelete={resolve}
            close={noop}
        />
    ),
};

export const EmptyRange: StoryObj<typeof DeleteTimeSeriesRangeModal> = {
    render: () => (
        <DeleteTimeSeriesRangeModal
            timeSeriesName="HeartRate"
            startDate={moment().subtract(7, "days")}
            endDate={moment()}
            resolveCount={countOf({ count: 0, exact: true })}
            onDelete={resolve}
            close={noop}
        />
    ),
};

export const NoActiveFilter: StoryObj<typeof DeleteTimeSeriesRangeModal> = {
    render: () => (
        <DeleteTimeSeriesRangeModal
            timeSeriesName="HeartRate"
            startDate={null}
            endDate={null}
            resolveCount={countOf({ count: 512, exact: true })}
            onDelete={resolve}
            close={noop}
        />
    ),
};

export const CountingSlowly: StoryObj<typeof DeleteTimeSeriesRangeModal> = {
    render: () => (
        <DeleteTimeSeriesRangeModal
            timeSeriesName="HeartRate"
            startDate={moment().subtract(7, "days")}
            endDate={moment()}
            resolveCount={() => new Promise(() => undefined)}
            onDelete={resolve}
            close={noop}
        />
    ),
};
