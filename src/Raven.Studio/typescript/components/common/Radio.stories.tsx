import { ComponentMeta } from "@storybook/react";
import { Radio } from "./Radio";
import useBoolean from "hooks/useBoolean";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { boundCopy } from "../utils/common";

export default {
    title: "Bits/Radio",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Radio,
} as ComponentMeta<typeof Radio>;

const Template = (args: { withLabel: boolean }) => {
    const { value: selected, toggle } = useBoolean(false);

    return (
        <div>
            <Radio toggleSelection={toggle}>First Radio</Radio>
        </div>
    );
};

export const WithLabel = boundCopy(Template);

WithLabel.args = {
    withLabel: true,
};
