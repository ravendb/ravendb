import React from "react";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import AdminJsConsole from "./AdminJsConsole";

export default {
    title: "Pages/AdminJsConsole",
    component: AdminJsConsole,
    decorators: [withStorybookContexts, withBootstrap5],
} as ComponentMeta<typeof AdminJsConsole>;

export const JSConsole: ComponentStory<typeof AdminJsConsole> = () => {
    return <AdminJsConsole />;
};
