import { ComponentMeta } from "@storybook/react";
import { HrBorder } from "./HrBorder";
import useBoolean from "hooks/useBoolean";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "../../test/storybookTestUtils";

export default {
    title: "Bits/HrBorder",
    component: HrBorder,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof HrBorder>;

export function HrBorders() {
    return <div>Test</div>;
}
