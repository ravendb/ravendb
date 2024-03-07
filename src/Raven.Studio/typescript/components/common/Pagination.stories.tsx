import { Meta } from "@storybook/react";
import DefaultPagination from "./Pagination";
import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Pagination",
    decorators: [withStorybookContexts, withBootstrap5],
    component: DefaultPagination,
} satisfies Meta<typeof DefaultPagination>;

export function Pagination() {
    return <DefaultPagination totalPages={122} />;
}
