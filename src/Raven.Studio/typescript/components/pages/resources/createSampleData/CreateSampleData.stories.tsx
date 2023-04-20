import React from "react";
import CreateSampleData from "./CreateSampleData";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Pages/Create sample data page",
    decorators: [withStorybookContexts, withBootstrap5],
};

export function FullView() {
    return <CreateSampleData />;
}
