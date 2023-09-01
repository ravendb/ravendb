import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import DatabaseCustomAnalyzers from "./DatabaseCustomAnalyzers";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Database/Settings",
    component: DatabaseCustomAnalyzers,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof DatabaseCustomAnalyzers>;

export function DefaultAnalyzers() {
    return <DatabaseCustomAnalyzers db={DatabasesStubs.nonShardedClusterDatabase()} />;
}
