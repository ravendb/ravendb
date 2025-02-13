import { Meta } from "@storybook/react";
import DefaultPagination from "./Pagination";
import React, { useState } from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Bits/Pagination",
    decorators: [withStorybookContexts, withBootstrap5],
    component: DefaultPagination,
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/ITHbe2U19Ok7cjbEzYa4cb/Design-System-RavenDB-Studio?node-id=10-101",
        },
    },
} satisfies Meta<typeof DefaultPagination>;

export function Pagination() {
    const [page, setPage] = useState(1);
    return <DefaultPagination totalPages={12} page={page} onPageChange={setPage} />;
}
