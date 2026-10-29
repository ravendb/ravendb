import { Meta, StoryObj } from "@storybook/react-webpack5";
import React from "react";
import moment from "moment";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import FilterTimeSeriesModal from "./FilterTimeSeriesModal";

export default {
    title: "Pages/Database/Time Series/Filter Time Series Modal",
    component: FilterTimeSeriesModal,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/eg11NPk6eP0QRTDFtnWv55/Pages---Documents?node-id=1200-5650",
        },
    },
} satisfies Meta<typeof FilterTimeSeriesModal>;

const noop = () => {
    // no-op for stories
};

export const Between: StoryObj<typeof FilterTimeSeriesModal> = {
    render: () => (
        <FilterTimeSeriesModal
            startDate={moment().subtract(7, "days")}
            endDate={moment()}
            onApply={noop}
            close={noop}
        />
    ),
};

export const Before: StoryObj<typeof FilterTimeSeriesModal> = {
    render: () => <FilterTimeSeriesModal startDate={null} endDate={moment()} onApply={noop} close={noop} />,
};

export const After: StoryObj<typeof FilterTimeSeriesModal> = {
    render: () => (
        <FilterTimeSeriesModal startDate={moment().subtract(30, "days")} endDate={null} onApply={noop} close={noop} />
    ),
};

export const Empty: StoryObj<typeof FilterTimeSeriesModal> = {
    render: () => <FilterTimeSeriesModal startDate={null} endDate={null} onApply={noop} close={noop} />,
};
