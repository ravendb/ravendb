import React from "react";
import { MethodGroup, SampleScript } from "components/common/sampleQueries/partials/sampleQueriesTypes";

export const scripts: SampleScript[] = [
    {
        title: "Filter out an array item",
        description: "Removes a specific line item (product ID 'products/1') from all order documents.",
        script: `from Orders \nupdate {\n    this.Lines = this.Lines.filter(l => l.Product != 'products/1');\n}`,
    },
    {
        title: "Denormalize the company name",
        description: "Adds a CompanyName field to all orders, populated from a related document.",
        script: `from Orders as o\nload o.Company as c\nupdate {\n    o.CompanyName = c.Name;\n}`,
    },
    {
        title: "Use JavaScript to patch",
        description: "Conditionally adds a new lowercase company name field to orders over 10,000.",
        script: `from index 'Orders/Totals' as i\nwhere i.Total > 10000\nload i.Company as c\nupdate { \n    i.LowerName = c.Name.toLowerCase();\n}`,
    },
    {
        title: "Access the metadata",
        description: "Adds the document ID and collection name as two new fields to every order.",
        script: `from Orders \nupdate {\n    this.DocumentId = id(this);\n    this.DocumentCollection = this["@metadata"]["@collection"];\n}`,
    },
    {
        title: "Add a time series entry",
        description: "Appends a time series entry to a specific document for every employee in the database.",
        script: `from Employees\nupdate {\n    timeseries("employees/1-A", "HeartRates")\n        .append("2020-06-25T10:48:14.794", [120, 80], "TagName");\n}`,
    },
];

export const methodGroups: MethodGroup[] = [
    {
        category: "Document operations",
        methods: [
            {
                signature: "id(document)",
                description: (
                    <>
                        Returns the ID (<code>string</code>) of the given document.
                    </>
                ),
            },
            {
                signature: "load(documentIdToLoad)",
                description: (
                    <>
                        Returns the document (<code>object</code>) with the given ID.
                    </>
                ),
            },
            {
                signature: "put(documentId, document)",
                description: (
                    <>
                        Creates or updates (<code>Task</code>) a document with the specified ID.
                    </>
                ),
            },
            {
                signature: "del(documentIdToRemove)",
                description: (
                    <>
                        Deletes (<code>void</code>) the document with the specified ID.
                    </>
                ),
            },
        ],
    },
    {
        category: "Counter operations",
        methods: [
            {
                signature: "counter(document/documentId, name)",
                description: (
                    <>
                        Get the counter value (<code>number</code>) by document/document ID.
                    </>
                ),
            },
            {
                signature: "incrementCounter(document/documentId, name, value = 1)",
                description: (
                    <>
                        Increment (<code>void</code>) a counter by document/document ID.
                    </>
                ),
            },
            {
                signature: "deleteCounter(document/documentId, name)",
                description: (
                    <>
                        Delete (<code>void</code>) a counter by document/document ID.
                    </>
                ),
            },
        ],
    },
    {
        category: "Time series",
        methods: [
            {
                signature: "timeseries(document/documentId, name).get(from, to)",
                description: (
                    <>
                        Get timeseries entries (<code>Promise</code>).
                    </>
                ),
            },
            {
                signature: "timeseries(document/documentId, name).append(timestamp, values, tag = null)",
                description: (
                    <>
                        Add a new entry (<code>Promise</code>) to a timeseries.
                    </>
                ),
            },
            {
                signature: "timeseries(document/documentId, name).delete(from, to)",
                description: (
                    <>
                        Delete entries (<code>{"Promise<void>"}</code>) from a timeseries.
                    </>
                ),
            },
        ],
    },
    {
        category: "General",
        methods: [
            {
                signature: "output(message)",
                description: (
                    <>
                        Output (<code>{"Promise<void>"}</code>) debug info when testing.
                    </>
                ),
            },
        ],
    },
];
