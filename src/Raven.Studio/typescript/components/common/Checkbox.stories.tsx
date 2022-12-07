import { ComponentMeta } from "@storybook/react";
import { Checkbox } from "./Checkbox";
import useBoolean from "hooks/useBoolean";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { boundCopy } from "../utils/common";

export default {
    title: "Bits/Checkbox",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Checkbox,
} as ComponentMeta<typeof Checkbox>;

const Template = (args: { withLabel: boolean }) => {
    const { value: selected, toggle } = useBoolean(false);

    return (
        <div>
            <Checkbox selected={selected} toggleSelection={toggle}>
                First checkbox
            </Checkbox>
        </div>
    );
};

export const WithLabel = boundCopy(Template);

WithLabel.args = {
    withLabel: true,
};
