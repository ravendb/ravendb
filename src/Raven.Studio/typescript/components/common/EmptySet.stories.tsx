import { Meta } from "@storybook/react";
import { EmptySet } from "./EmptySet";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/EmptySet",
    component: EmptySet,
    decorators: [withStorybookContexts, withBootstrap5],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=8-6692",
        },
    },
} satisfies Meta<typeof EmptySet>;

export function EmptySets() {
    return (
        <div>
            <EmptySet>Use whenever a list is empty</EmptySet>
            <EmptySet compact>Use whenever a list is empty</EmptySet>
        </div>
    );
}
