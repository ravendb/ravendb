import { Meta, StoryObj } from "@storybook/react-webpack5";
import document from "models/database/documents/document";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { CellWithDocumentPreview, DocumentPreview } from "./CellWithDocumentPreview";

export default {
    title: "Bits/Virtual Table/Document preview",
    component: CellWithDocumentPreview,
    decorators: [withStorybookContexts, withBootstrap5],
} satisfies Meta<typeof CellWithDocumentPreview>;

interface PreviewState {
    label: string;
    id: string;
    load: () => Promise<DocumentPreview | null>;
}

const PAGE_VIEW_ID =
    "users/7c9e6679-7425-40de-944b-e07fc1f90ae7/sessions/2026-11-16T10:42:17.5531234Z/page-views/2026-11-16T10:43:02.1180412Z";
const SLOW_LOAD_IN_MS = 3_000;
const ID_COLUMN_WIDTH_IN_PX = 240;

const CUT_ORDER: PreviewState = {
    label: "Cut document with every badge",
    id: "orders/1-A",
    load: async () => ({ document: DatabasesStubs.documentWithMetadata("orders/1-A"), revisionsCount: 29 }),
};

const PREVIEW_STATES: PreviewState[] = [
    CUT_ORDER,
    {
        label: "Short document",
        id: "categories/1-A",
        load: async () => ({ document: category(), revisionsCount: 0 }),
    },
    {
        label: "Long values",
        id: "employees/1-A",
        load: async () => ({ document: employee(), revisionsCount: 3 }),
    },
    {
        label: "Long ID, two lines at most",
        id: PAGE_VIEW_ID,
        load: async () => ({ document: pageView(), revisionsCount: 0 }),
    },
    {
        label: "Slow to load",
        id: "orders/830-A",
        load: () =>
            new Promise((resolve) =>
                setTimeout(
                    () =>
                        resolve({ document: DatabasesStubs.documentWithMetadata("orders/830-A"), revisionsCount: 29 }),
                    SLOW_LOAD_IN_MS
                )
            ),
    },
    {
        label: "Document no longer exists",
        id: "orders/831-A",
        load: async () => null,
    },
    {
        label: "Document can't be loaded",
        id: "orders/832-A",
        load: () => Promise.reject(new Error("Internal Server Error")),
    },
];

export const States: StoryObj = {
    render: () => (
        <div className="vstack gap-3 p-3">
            {PREVIEW_STATES.map((state) => (
                <PreviewCell key={state.id} {...state} />
            ))}
        </div>
    ),
};

export const WindowEdges: StoryObj = {
    name: "Window edges",
    render: () => (
        <div className="vstack justify-content-between p-3" style={{ minHeight: "100vh" }}>
            <PreviewCell {...CUT_ORDER} label="Top of the window" />
            <PreviewCell {...CUT_ORDER} label="Bottom of the window" />
        </div>
    ),
};

function PreviewCell({ label, id, load }: PreviewState) {
    return (
        <div style={{ width: ID_COLUMN_WIDTH_IN_PX }}>
            <div className="small-label">{label}</div>
            <CellWithDocumentPreview id={id} previewSource={{ load }}>
                <div className="text-truncate">{id}</div>
            </CellWithDocumentPreview>
        </div>
    );
}

function category() {
    return new document({
        "@metadata": { "@collection": "Categories", "@id": "categories/1-A" },
        Name: "Beverages",
        Description: "Soft drinks, coffees, teas, beers, and ales",
    });
}

function employee() {
    return new document({
        "@metadata": {
            "@collection": "Employees",
            "@id": "employees/1-A",
            "@flags": "HasRevisions, HasTimeSeries",
            "@timeseries": ["HeartRates"],
        },
        LastName: "Davolio",
        FirstName: "Nancy",
        Title: "Sales Representative",
        Notes: [
            "Education includes a BA in psychology from Colorado State University in 1970. She also completed The Art of the Cold Call. Nancy is a member of Toastmasters International.",
        ],
        Photo: "iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAA".repeat(2_000),
    });
}

function pageView() {
    return new document({
        "@metadata": { "@collection": "PageViews", "@id": PAGE_VIEW_ID },
        Session: "users/7c9e6679-7425-40de-944b-e07fc1f90ae7/sessions/2026-11-16T10:42:17.5531234Z",
        Url: "/studio/index.html#databases/documents?collection=Orders&database=Northwind",
        ViewedAt: "2026-11-16T10:43:02.1180412Z",
    });
}
