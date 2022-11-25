import { ComponentMeta } from "@storybook/react";
import { Checkbox } from "./Checkbox";
import useBoolean from "hooks/useBoolean";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "../../test/storybookTestUtils";

export default {
    title: "Bits/Checkbox",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Checkbox,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof Checkbox>;

export function Checkboxes() {
    const { value: selected, toggle } = useBoolean(false);

    return (
        <div>
            <Checkbox selected={selected} toggleSelection={toggle}>
                First checkbox
            </Checkbox>
        </div>
    );
}
