import { ComponentMeta } from "@storybook/react";
import { StickyHeader } from "./StickyHeader";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/StickyHeader",
    component: StickyHeader,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof StickyHeader>;

const loremIpsum = `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam sed tincidunt odio. Aliquam a diam
tristique, consectetur arcu eu, finibus nibh. Sed libero lacus, posuere ac congue sed, ullamcorper in
massa. Nunc aliquet feugiat tortor, interdum imperdiet nibh maximus et. Nullam eget finibus odio.`;

export function StickyHeaders() {
    return (
        <div style={{ overflow: "auto", height: "90vh" }}>
            <StickyHeader>Header with divider</StickyHeader>
            <div style={{ height: "200vh" }}>{loremIpsum.repeat(6)}</div>
        </div>
    );
}
