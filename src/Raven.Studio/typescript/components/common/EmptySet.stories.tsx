import { ComponentMeta } from "@storybook/react";
import { EmptySet } from "./EmptySet";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/EmptySet",
    component: EmptySet,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof EmptySet>;

export function EmptySets() {
    return (
        <div>
            <EmptySet>Use whenever a list is empty</EmptySet>
        </div>
    );
}
