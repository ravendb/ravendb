import React, { useMemo } from "react";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { Icon } from "components/common/Icon";
import router from "plugins/router";
import SampleQueriesPage from "components/common/sampleQueries/SampleQueriesPage";
import { MethodGroup, SampleScript } from "components/common/sampleQueries/partials/sampleQueriesTypes";
import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import savedPatchesStorage from "common/storage/savedPatchesStorage";
import messagePublisher from "common/messagePublisher";

const scripts: SampleScript[] = [
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

const methodGroups: MethodGroup[] = [
    {
        category: "Document operations",
        methods: [
            {
                signature: "id(document)",
                description: "Returns the ID of the given document.",
                returnType: "string",
            },
            {
                signature: "load(documentIdToLoad)",
                description: "Returns the document with the given ID.",
                returnType: "object",
            },
            {
                signature: "put(documentId, document)",
                description: "Creates or updates a document with the specified ID.",
                returnType: "Task",
            },
            {
                signature: "del(documentIdToRemove)",
                description: "Deletes the document with the specified ID.",
                returnType: "void",
            },
        ],
    },
    {
        category: "Counter operations",
        methods: [
            {
                signature: "counter(document/documentId, name)",
                description: "Get the counter value by document/document ID.",
                returnType: "number",
            },
            {
                signature: "incrementCounter(document/documentId, name, value = 1)",
                description: "Increment a counter by document/document ID.",
                returnType: "void",
            },
            {
                signature: "deleteCounter(document/documentId, name)",
                description: "Delete a counter by document/document ID.",
                returnType: "void",
            },
        ],
    },
    {
        category: "Time series",
        methods: [
            {
                signature: "timeseries(document/documentId, name).get(from, to)",
                description: "Get timeseries entries.",
                returnType: "Promise",
            },
            {
                signature: "timeseries(document/documentId, name).append(timestamp, values, tag = null)",
                description: "Add a new entry to a timeseries.",
                returnType: "Promise",
            },
            {
                signature: "timeseries(document/documentId, name).delete(from, to)",
                description: "Delete entries from a timeseries.",
                returnType: "Promise<void>",
            },
        ],
    },
    {
        category: "General",
        methods: [
            {
                signature: "output(message)",
                description: "Output debug info when testing.",
                returnType: "Promise<void>",
            },
        ],
    },
];

function PatchSampleQueriesAboutView() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p className="mb-0">TODO</p>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}

interface PatchSampleQueriesQueryParams {
    initialScriptHash?: string;
}

export default function PatchSampleQueriesPage({ queryParams }: ReactQueryParamsProps<PatchSampleQueriesQueryParams>) {
    const { appUrl } = useAppUrls();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const backUrl = appUrl.forPatch(databaseName);

    const initialScript = useMemo(() => {
        const hashStr = queryParams?.initialScriptHash;

        if (!hashStr || !databaseName) {
            return null;
        }

        const hash = parseInt(hashStr, 10);

        if (isNaN(hash)) {
            return null;
        }

        return savedPatchesStorage.getPlaygroundScript(databaseName, hash);
    }, [databaseName]);

    const handleUpdateScript = (script: string) => {
        if (databaseName) {
            try {
                const hash = savedPatchesStorage.storePlaygroundScript(databaseName, script);
                router.navigate(appUrl.forPatch(databaseName, hash));
            } catch {
                messagePublisher.reportError("Failed to save patch script");
            }
        }
    };

    return (
        <SampleQueriesPage
            title={
                <>
                    <Icon icon="info" />
                    Patch scripts playground
                </>
            }
            scripts={scripts}
            methodGroups={methodGroups}
            backUrl={backUrl}
            initialScript={initialScript}
            aboutView={<PatchSampleQueriesAboutView />}
            onUpdateScript={handleUpdateScript}
        />
    );
}
