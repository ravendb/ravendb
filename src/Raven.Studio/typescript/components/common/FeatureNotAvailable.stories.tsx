import React from "react";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import FeatureNotAvailable from "./FeatureNotAvailable";
import { Icon } from "./Icon";

export default {
    title: "Bits/Feature Not Available",
    component: FeatureNotAvailable,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=479-1178",
        },
    },
} satisfies Meta<typeof FeatureNotAvailable>;

export const Default: StoryObj<typeof FeatureNotAvailable> = {
    name: "Feature Not Available",
    render: () => {
        return (
            <FeatureNotAvailable>
                <span>
                    Import documents from a CSV file into a collection is not available for{" "}
                    <Icon icon="sharding" color="shard" margin="m-0" /> sharded databases
                </span>
            </FeatureNotAvailable>
        );
    },
};
