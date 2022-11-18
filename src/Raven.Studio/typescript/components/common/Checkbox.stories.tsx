import { ComponentMeta } from "@storybook/react";
import { Checkbox } from "./Checkbox";
import useBoolean from "hooks/useBoolean";
import React from "react";

export default {
    title: "Bits/Checkbox",
    component: Checkbox,
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
