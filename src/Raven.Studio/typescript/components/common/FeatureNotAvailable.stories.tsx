import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import FeatureNotAvailable from "./FeatureNotAvailable";

export default {
    title: "Bits",
    component: FeatureNotAvailable,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof FeatureNotAvailable>;

export const Default: StoryObj<typeof FeatureNotAvailable> = {
    name: "Feature Not Available",
    render: () => {
        return <FeatureNotAvailable />;
    },
};
