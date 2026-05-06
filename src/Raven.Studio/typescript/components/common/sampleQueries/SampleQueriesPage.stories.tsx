import React from "react";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import SampleQueriesPage from "./SampleQueriesPage";
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
                description: (
                    <>
                        Returns the document (<code>object</code>) with the given ID.
                    </>
                ),
            },
        ],
    },
];

export const Default: StoryObj = {
    name: "Sample Queries",
    render: () => {
        return (
            <SampleQueriesPage
                title="Sample Queries"
                icon="patch"
                scripts={sampleScripts}
                methodGroups={sampleMethodGroups}
                onClose={() => {
                    /* no-op */
                }}
                onUpdateScript={() => {
                    /* no-op */
                }}
            />
        );
    },
};
