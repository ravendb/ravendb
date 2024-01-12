import { Meta } from "@storybook/react";
import CreateDatabase from "./CreateDatabase";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Pages/Databases/Create",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

export const Create = {
    render: () => {
        return <CreateDatabase closeModal={() => null} />;
    },
};
