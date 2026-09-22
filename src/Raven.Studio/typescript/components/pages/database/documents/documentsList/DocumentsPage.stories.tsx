import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5, databaseAccessArgType } from "test/storybookTestUtils";
import DocumentsPage from "./DocumentsPage";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import document from "models/database/documents/document";

export default {
    title: "Pages/Database/Documents/DocumentsPage",
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta;

interface DocumentsListStoryArgs {
    collection: string;
    isSharded: boolean;
    totalCount: number;
    databaseAccess: databaseAccessLevel;
}

export const DocumentsListStory: StoryObj<DocumentsListStoryArgs> = {
    name: "Documents List",
    render: ({ collection, isSharded, totalCount, databaseAccess }) => {
        const { name } = isSharded
            ? mockStore.databases.withActiveDatabase_Sharded()
            : mockStore.databases.withActiveDatabase();
        mockStore.accessManager.with_databaseAccess({ [name]: databaseAccess });
        mockStore.collectionsTracker.with_Collections();
        mockServices.databasesService.withGeneratedDocumentsPreview(totalCount);

        return (
            <div style={{ height: "600px" }}>
                <DocumentsPage queryParams={{ collection: collection || undefined }} />
            </div>
        );
    },
    args: {
        collection: "Orders",
        isSharded: false,
        totalCount: 10_000_000,
        databaseAccess: "DatabaseAdmin",
    },
    argTypes: {
        databaseAccess: databaseAccessArgType,
    },
};

interface AllDocumentsStoryArgs {
    isEmpty: boolean;
}

export const AllDocumentsStory: StoryObj<AllDocumentsStoryArgs> = {
    name: "All Documents",
    render: ({ isEmpty }) => {
        const { name } = mockStore.databases.withActiveDatabase();
        mockStore.accessManager.with_databaseAccess({ [name]: "DatabaseAdmin" });
        mockStore.collectionsTracker.with_Collections();
        mockServices.databasesService.withDocumentsPreview(
            isEmpty
                ? { items: [], totalResultCount: 0, availableColumns: [], continuationToken: null, resultEtag: "1" }
                : undefined
        );

        return (
            <div style={{ height: "600px" }}>
                <DocumentsPage queryParams={{}} />
            </div>
        );
    },
    args: {
        isEmpty: false,
    },
};

// the preview trims long values, hovering such a cell fetches the whole document for the preview popover
export const TrimmedValuesStory: StoryObj = {
    name: "Trimmed values",
    render: () => {
        const { name } = mockStore.databases.withActiveDatabase();
        mockStore.accessManager.with_databaseAccess({ [name]: "DatabaseAdmin" });
        mockStore.collectionsTracker.with_Collections();

        const [trimmedDocument] = DatabasesStubs.documentsPreviewItems(0, 1);
        (trimmedDocument as unknown as Record<string, unknown>)["Company"] = "compa...";
        (trimmedDocument.__metadata as unknown as Record<string, unknown>)["$t"] = ["Company"];

        mockServices.databasesService.withDocumentsPreview({
            items: [trimmedDocument],
            totalResultCount: 1,
            availableColumns: DatabasesStubs.documentsPreviewColumns(),
            continuationToken: null,
            resultEtag: "1",
        });
        mockServices.databasesService.withDocumentWithMetadata(
            new document({
                "@metadata": { "@id": "orders/1-A", "@collection": "Orders" },
                Company: "companies/1-A",
            })
        );

        return (
            <div style={{ height: "600px" }}>
                <DocumentsPage queryParams={{ collection: "Orders" }} />
            </div>
        );
    },
};
