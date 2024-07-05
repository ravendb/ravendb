import React from "react";
import { Meta, StoryFn } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import AceEditor from "./AceEditor";

export default {
    title: "Bits/AceEditor",
    component: AceEditor,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof AceEditor>;

export const JavascriptEditor: StoryFn<typeof AceEditor> = () => {
    return <AceEditor mode="javascript" />;
};
