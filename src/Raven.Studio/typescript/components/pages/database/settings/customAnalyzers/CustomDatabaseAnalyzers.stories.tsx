import React from "react";
import { Meta } from "@storybook/react";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";
import CustomDatabaseAnalyzers from "./CustomDatabaseAnalyzers";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default {
    title: "Pages/Database/Settings",
    component: CustomDatabaseAnalyzers,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof CustomDatabaseAnalyzers>;

export function DefaultAnalyzers() {
    return <CustomDatabaseAnalyzers db={DatabasesStubs.nonShardedClusterDatabase()} />;
}
