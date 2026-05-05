import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import SampleQueriesPage from "./SampleQueriesPage";
import { mockStore } from "test/mocks/store/MockStore";
import { SampleScript, MethodGroup } from "./partials/sampleQueriesTypes";

export default {
    title: "Pages/Database/Sample Queries",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

const sampleScripts: SampleScript[] = [
    {
        title: "Filter out an array item",
        description: "Removes a specific line item (product ID 'products/1') from all order documents.",
        script: `from Orders \nupdate {\n    this.Lines = this.Lines.filter(l => l.Product != 'products/1');\n}`,
    },
];

const sampleMethodGroups: MethodGroup[] = [
    {
        category: "Document operations",
        methods: [
            {
                signature: "load(documentIdToLoad)",
                description: "Returns the document with the given ID.",
                returnType: "object",
            },
        ],
    },
];

export const Default: StoryObj = {
    name: "Sample Queries",
    render: () => {
        mockStore.databases.withActiveDatabase_NonSharded_SingleNode();

        return (
            <SampleQueriesPage
                title="Sample Queries"
                scripts={sampleScripts}
                methodGroups={sampleMethodGroups}
                backUrl="#"
                onUpdateScript={() => {
                    /* no-op */
                }}
            />
        );
    },
};
