import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import AceEditor from "./AceEditor";

export default {
    title: "Bits/AceEditor",
    component: AceEditor,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof AceEditor>;

export const JavascriptEditor: ComponentStory<typeof AceEditor> = () => {
    return <AceEditor mode="javascript" />;
};
