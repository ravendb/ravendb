import { rtlRender, fireEvent, within, act } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import * as Stories from "./DocumentsPage.stories";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import appUrl from "common/appUrl";
import messagePublisher from "common/messagePublisher";
import router from "plugins/router";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DocumentsListStory, AllDocumentsStory, TrimmedValuesStory } = composeStories(Stories);

type Screen = ReturnType<typeof rtlRender>["screen"];

const getSelectAllCheckbox = (screen: Screen) => screen.getByRole("checkbox", { name: "Select all documents" });
const getDocumentCheckboxes = (screen: Screen) => screen.getAllByRole("checkbox", { name: "Select document" });
const getSelectionActions = (screen: Screen) => screen.queryByTestId("selection-actions");
const getRowCheckbox = (screen: Screen, documentId: string) =>
    within(screen.getByText(documentId).closest("tr")).getByRole("checkbox", { name: "Select document" });

const openDisplayDropdown = (screen: Screen) => {
    fireEvent.click(screen.getByRole("button", { name: /Display/ }));
};

// the mocked preview resolves in a microtask, flushing it inside act keeps the resulting state updates in the test
const flushFetches = () => act(async () => {});

const openColumnSettings = async (screen: Screen) => {
    openDisplayDropdown(screen);
    fireEvent.click(await screen.findByText("Column layout settings"));
    return await screen.findByText("Set up your column layout");
};

const addCustomCityColumn = async (screen: Screen) => {
    await openColumnSettings(screen);
    fireEvent.click(screen.getByRole("button", { name: /Add a custom column/ }));
    fireEvent.change(screen.getByLabelText("Binding expression"), { target: { value: "this.ShipTo.City" } });
    fireEvent.change(screen.getByLabelText("Alias"), { target: { value: "City" } });
    fireEvent.click(screen.getByRole("button", { name: /Save column/ }));
    fireEvent.click(screen.getByRole("button", { name: "Apply" }));
    await flushFetches();
};

const getColumnLayoutStorageKeys = () =>
    Object.keys(localStorage).filter((key) => key.includes("documents-columns-") && key.includes("[Orders]"));

describe("DocumentsPage", () => {
    // the ResizeObserver polyfill reports a 100px wide table in jsdom, which would hide all property columns
    // without it the table keeps the 500px width mocked by getBoundingClientRect
    beforeAll(() => {
        window.ResizeObserver = class {
            observe() {}
            unobserve() {}
            disconnect() {}
        };
    });

    it("can render collection documents with property columns", async () => {
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(screen.getByText("Company")).toBeInTheDocument();
        expect(screen.getByText("@id")).toBeInTheDocument();
    });

    it("can render all documents with metadata columns and offers the property columns", async () => {
        const { screen } = rtlRender(<AllDocumentsStory />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(screen.getByText("Change Vector")).toBeInTheDocument();
        expect(screen.getByText("Last Modified")).toBeInTheDocument();
        expect(screen.getByText("Collection")).toBeInTheDocument();
        expect(screen.queryByText("Company")).not.toBeInTheDocument();

        await openColumnSettings(screen);

        expect(screen.getByRole("checkbox", { name: "Company" })).not.toBeChecked();
        expect(screen.getByRole("checkbox", { name: "Collection" })).toBeChecked();
        expect(screen.queryByRole("checkbox", { name: "__metadata" })).not.toBeInTheDocument();
    });

    it("refetches the documents with the enabled property column as a preview binding", async () => {
        const { screen } = rtlRender(<AllDocumentsStory />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

        const getDocumentsPreview = mockServices.databasesService.getMock("getDocumentsPreview");
        const fetchCount = getDocumentsPreview.mock.calls.length;

        await openColumnSettings(screen);
        fireEvent.click(screen.getByRole("checkbox", { name: "Company" }));
        fireEvent.click(screen.getByRole("button", { name: "Apply" }));
        await flushFetches();

        expect(await screen.findByText("Company")).toBeInTheDocument();
        expect(getDocumentsPreview.mock.calls.length).toBeGreaterThan(fetchCount);

        const lastCall = getDocumentsPreview.mock.calls[getDocumentsPreview.mock.calls.length - 1];
        expect(lastCall[4]).toEqual(["Company"]);
    });

    it("can render sharded database documents", async () => {
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
    });

    it("renders the toolbar actions", async () => {
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /New document/ })).toBeEnabled();
        expect(screen.getByRole("button", { name: /Query/ })).toBeInTheDocument();
        expect(getSelectionActions(screen)).not.toBeInTheDocument();

        openDisplayDropdown(screen);

        expect(await screen.findByText("Column layout settings")).toBeInTheDocument();
        expect(screen.getByRole("checkbox", { name: "Pagination" })).not.toBeChecked();
    });

    it("selects every document from the header checkbox", async () => {
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

        fireEvent.click(getSelectAllCheckbox(screen));

        getDocumentCheckboxes(screen).forEach((checkbox) => expect(checkbox).toBeChecked());
        expect(getSelectAllCheckbox(screen)).toBeChecked();
        expect(within(getSelectionActions(screen)).getByText("128")).toBeInTheDocument();
        expect(within(getSelectionActions(screen)).getByRole("button", { name: /Delete/ })).toBeEnabled();

        fireEvent.click(getDocumentCheckboxes(screen)[1]);

        expect(getDocumentCheckboxes(screen)[1]).not.toBeChecked();
        expect(getSelectAllCheckbox(screen)).not.toBeChecked();
        expect(getSelectAllCheckbox(screen)).toBePartiallyChecked();

        fireEvent.click(within(getSelectionActions(screen)).getByRole("button", { name: "Clear selection" }));

        getDocumentCheckboxes(screen).forEach((checkbox) => expect(checkbox).not.toBeChecked());
        expect(getSelectionActions(screen)).not.toBeInTheDocument();
    });

    it("selects the current page from the header checkbox when paginated and clears the selection on toggle", async () => {
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={1000} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

        fireEvent.click(getSelectAllCheckbox(screen));
        expect(within(getSelectionActions(screen)).getByText("128")).toBeInTheDocument();

        openDisplayDropdown(screen);
        fireEvent.click(await screen.findByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText(/^1-\d+ of 1,000$/)).toBeInTheDocument();
        expect(getSelectionActions(screen)).not.toBeInTheDocument();
        expect(getSelectAllCheckbox(screen)).not.toBeChecked();

        fireEvent.click(getSelectAllCheckbox(screen));

        const pageCheckboxes = getDocumentCheckboxes(screen);
        const pageSize = pageCheckboxes.length;
        expect(pageSize).toBeLessThan(128);
        pageCheckboxes.forEach((checkbox) => expect(checkbox).toBeChecked());
        expect(getSelectAllCheckbox(screen)).toBeChecked();
        expect(within(getSelectionActions(screen)).getByText(String(pageSize))).toBeInTheDocument();

        fireEvent.click(pageCheckboxes[0]);

        expect(getSelectAllCheckbox(screen)).toBePartiallyChecked();

        fireEvent.click(pageCheckboxes[0]);
        fireEvent.click(screen.getByRole("button", { name: "Next page" }));
        await flushFetches();

        expect(getSelectAllCheckbox(screen)).not.toBeChecked();
        expect(getSelectAllCheckbox(screen)).not.toBePartiallyChecked();
        expect(within(getSelectionActions(screen)).getByText(String(pageSize))).toBeInTheDocument();

        fireEvent.click(getSelectAllCheckbox(screen));

        expect(within(getSelectionActions(screen)).getByText(String(2 * pageSize))).toBeInTheDocument();

        fireEvent.click(getSelectAllCheckbox(screen));

        expect(getSelectAllCheckbox(screen)).not.toBeChecked();
        getDocumentCheckboxes(screen).forEach((checkbox) => expect(checkbox).not.toBeChecked());
        expect(within(getSelectionActions(screen)).getByText(String(pageSize))).toBeInTheDocument();

        openDisplayDropdown(screen);
        fireEvent.click(await screen.findByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(getSelectionActions(screen)).not.toBeInTheDocument();
        expect(getSelectAllCheckbox(screen)).not.toBeChecked();
    });

    it("selects a range of documents while holding shift and highlights the pending range", async () => {
        const { screen, container } = rtlRender(
            <DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />
        );

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

        const checkboxes = getDocumentCheckboxes(screen);
        expect(checkboxes).toHaveLength(5);

        fireEvent.click(checkboxes[1]);

        const rows = container.querySelectorAll("tbody tr");
        fireEvent.mouseMove(rows[3], { shiftKey: true });

        expect(rows[0]).not.toHaveClass("selection-preview");
        expect(rows[1]).toHaveClass("selection-preview");
        expect(rows[2]).toHaveClass("selection-preview");
        expect(rows[3]).toHaveClass("selection-preview");
        expect(rows[4]).not.toHaveClass("selection-preview");

        fireEvent.click(checkboxes[3], { shiftKey: true });

        expect(checkboxes[0]).not.toBeChecked();
        expect(checkboxes[1]).toBeChecked();
        expect(checkboxes[2]).toBeChecked();
        expect(checkboxes[3]).toBeChecked();
        expect(checkboxes[4]).not.toBeChecked();
        expect(rows[2]).toHaveClass("is-selected");
        expect(within(getSelectionActions(screen)).getByText("3")).toBeInTheDocument();
        expect(within(getSelectionActions(screen)).getByRole("button", { name: "Copy" })).toBeEnabled();

        fireEvent.keyUp(document, { key: "Shift", shiftKey: false });

        expect(rows[2]).not.toHaveClass("selection-preview");
    });

    it("toggles the clicked document after scrolling shifted the loaded rows", async () => {
        const { screen, container } = rtlRender(
            <DocumentsListStory collection="Orders" isSharded={false} totalCount={1000} />
        );

        expect(await screen.findByText("orders/16-A")).toBeInTheDocument();

        const scrollContainer = container.querySelector<HTMLDivElement>(".table-container");
        fireEvent.scroll(scrollContainer, { target: { scrollTop: 1200 } });

        expect(await screen.findByText("orders/40-A")).toBeInTheDocument();

        fireEvent.click(getRowCheckbox(screen, "orders/16-A"));

        expect(getRowCheckbox(screen, "orders/16-A")).toBeChecked();
        expect(getRowCheckbox(screen, "orders/26-A")).not.toBeChecked();
    });

    it("selects the whole shift range when its start is scrolled out of view", async () => {
        const { screen, container } = rtlRender(
            <DocumentsListStory collection="Orders" isSharded={false} totalCount={1000} />
        );

        expect(await screen.findByText("orders/3-A")).toBeInTheDocument();

        fireEvent.click(getRowCheckbox(screen, "orders/3-A"));

        const scrollContainer = container.querySelector<HTMLDivElement>(".table-container");
        fireEvent.scroll(scrollContainer, { target: { scrollTop: 2400 } });

        expect(await screen.findByText("orders/71-A")).toBeInTheDocument();
        expect(screen.queryByText("orders/3-A")).not.toBeInTheDocument();

        fireEvent.click(getRowCheckbox(screen, "orders/71-A"), { shiftKey: true });

        expect(within(getSelectionActions(screen)).getByText("69")).toBeInTheDocument();
    });

    it("can toggle pagination from the display dropdown", async () => {
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

        openDisplayDropdown(screen);
        fireEvent.click(await screen.findByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText("1-5 of 5")).toBeInTheDocument();
        expect(screen.queryByText("Turn off pagination")).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        expect(screen.queryByText("1-5 of 5")).not.toBeInTheDocument();
        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
    });

    it("can add a custom column from the column layout settings", async () => {
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

        await openColumnSettings(screen);
        fireEvent.click(screen.getByRole("button", { name: /Add a custom column/ }));

        fireEvent.click(screen.getByRole("button", { name: /Save column/ }));
        expect(screen.getByText("The binding expression is required")).toBeInTheDocument();
        expect(screen.getByText("The alias is required")).toBeInTheDocument();

        fireEvent.change(screen.getByLabelText("Binding expression"), { target: { value: "this.ShipTo.City +" } });
        fireEvent.change(screen.getByLabelText("Alias"), { target: { value: "City" } });
        fireEvent.click(screen.getByRole("button", { name: /Save column/ }));
        expect(screen.queryByText("The binding expression is required")).not.toBeInTheDocument();
        expect(screen.getByTestId("custom-column-form")).toBeInTheDocument();

        fireEvent.change(screen.getByLabelText("Binding expression"), { target: { value: "this.ShipTo.City" } });
        fireEvent.click(screen.getByRole("button", { name: /Save column/ }));

        expect(screen.queryByTestId("custom-column-form")).not.toBeInTheDocument();
        expect(screen.getByRole("checkbox", { name: "City" })).toBeChecked();

        fireEvent.click(screen.getByRole("button", { name: "Apply" }));
        await flushFetches();

        expect(await screen.findByText("City")).toBeInTheDocument();
        expect((await screen.findAllByText("Reims")).length).toBeGreaterThan(0);

        const getDocumentsPreview = mockServices.databasesService.getMock("getDocumentsPreview");
        const lastCall = getDocumentsPreview.mock.calls[getDocumentsPreview.mock.calls.length - 1];
        expect(lastCall[5]).toEqual(["ShipTo"]);
    });

    it("shows the data changed alert when the collection changes and hides it after refresh", async () => {
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(screen.queryByTestId("data-changed-alert")).not.toBeInTheDocument();

        act(() => {
            mockStore.collectionsTracker.with_Collections([
                { name: "Orders", documentCount: 129, lastDocumentChangeVector: "A:2-abc" },
            ]);
        });

        expect(await screen.findByTestId("data-changed-alert")).toBeInTheDocument();
        expect(screen.getByText(/The data has changed/)).toBeInTheDocument();

        const getDocumentsPreview = mockServices.databasesService.getMock("getDocumentsPreview");
        const fetchCount = getDocumentsPreview.mock.calls.length;

        fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
        await flushFetches();

        expect(screen.queryByTestId("data-changed-alert")).not.toBeInTheDocument();
        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(getDocumentsPreview.mock.calls.length).toBeGreaterThan(fetchCount);
    });

    it("exports the collection to a file with the chosen format and columns", async () => {
        const submit = jest.spyOn(HTMLFormElement.prototype, "submit").mockImplementation(() => {});
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

        fireEvent.click(screen.getByRole("button", { name: /Export to file/ }));
        fireEvent.click(await screen.findByRole("button", { name: /^Export$/ }));

        const form = screen.getByTestId("export-form") as HTMLFormElement;
        expect(submit).toHaveBeenCalledTimes(1);
        expect(form.action).toContain("/streams/queries?format=csv");
        expect(form.action).not.toContain("field=");
        expect(form.querySelector<HTMLInputElement>("[name=ExportOptions]").value).toBe(
            JSON.stringify({ Query: "from 'Orders'" })
        );

        fireEvent.click(screen.getByRole("button", { name: /Export to file/ }));
        fireEvent.click(await screen.findByRole("radio", { name: "JSON" }));
        fireEvent.click(screen.getByRole("radio", { name: "Visible" }));
        fireEvent.click(screen.getByRole("button", { name: /^Export$/ }));

        expect(submit).toHaveBeenCalledTimes(2);
        expect(form.action).toContain("/streams/queries?format=json&field=%40id&field=Company");
        expect(form.action).not.toContain("field=__metadata");
    });

    it("does not offer the export to file for all documents", async () => {
        const { screen } = rtlRender(<AllDocumentsStory />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Export to file/ })).not.toBeInTheDocument();
    });

    it("redirects to all documents with a warning when the shown collection is removed", async () => {
        const navigate = jest.spyOn(router, "navigate").mockImplementation(() => true);
        const reportWarning = jest.spyOn(messagePublisher, "reportWarning").mockImplementation(() => {});
        const { screen } = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(navigate).not.toHaveBeenCalled();

        act(() => {
            mockStore.collectionsTracker.with_CollectionsExcept(["Orders"]);
        });

        expect(reportWarning).toHaveBeenCalledWith("Orders was removed");
        expect(navigate).toHaveBeenCalledWith(expect.stringContaining(appUrl.forDocuments(null, null)));
        expect(navigate.mock.calls[0][0]).not.toContain("collection=");
    });

    it("keeps the column layout of the collection between visits", async () => {
        localStorage.clear();
        const { screen, unmount } = rtlRender(
            <DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />
        );

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /Display/ })).not.toHaveClass("active");

        await addCustomCityColumn(screen);

        expect(await screen.findByText("City")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /Display/ })).toHaveClass("active");
        expect(getColumnLayoutStorageKeys()).toHaveLength(1);

        unmount();

        const secondVisit = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={5} />);

        expect(await secondVisit.screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(secondVisit.screen.getByText("City")).toBeInTheDocument();
        expect(secondVisit.screen.getByRole("button", { name: /Display/ })).toHaveClass("active");

        const getDocumentsPreview = mockServices.databasesService.getMock("getDocumentsPreview");
        const lastCall = getDocumentsPreview.mock.calls[getDocumentsPreview.mock.calls.length - 1];
        expect(lastCall[5]).toEqual(["ShipTo"]);

        await openColumnSettings(secondVisit.screen);
        fireEvent.click(secondVisit.screen.getByRole("button", { name: /Restart to default/ }));
        await flushFetches();

        expect(secondVisit.screen.queryByText("City")).not.toBeInTheDocument();
        expect(secondVisit.screen.getByRole("button", { name: /Display/ })).not.toHaveClass("active");
        expect(getColumnLayoutStorageKeys()).toHaveLength(0);
    });

    it("shows the empty messages for an empty collection and an empty database", async () => {
        const collectionView = rtlRender(<DocumentsListStory collection="Orders" isSharded={false} totalCount={0} />);
        expect(await collectionView.screen.findByText("Collection is empty")).toBeInTheDocument();
        collectionView.unmount();

        const allDocumentsView = rtlRender(<AllDocumentsStory isEmpty />);
        expect(await allDocumentsView.screen.findByText("There are no documents in the database")).toBeInTheDocument();
    });

    it("fetches the full document for the preview of a stubbed value", async () => {
        jest.useFakeTimers();

        try {
            const { screen } = rtlRender(<TrimmedValuesStory />);

            expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

            fireEvent.mouseEnter(screen.getByText("compa...").closest(".cell-preview-target"));
            act(() => {
                jest.advanceTimersByTime(200);
            });

            expect(await screen.findByText(/companies\/1-A/)).toBeInTheDocument();
            expect(mockServices.databasesService.getMock("getDocumentWithMetadata")).toHaveBeenCalledWith(
                "orders/1-A",
                expect.any(String),
                true
            );
        } finally {
            jest.useRealTimers();
        }
    });

    it("keeps a selected column when the documents scrolled to no longer have it", async () => {
        localStorage.clear();
        const totalCount = 5000;
        const { screen, container } = rtlRender(
            <DocumentsListStory collection="Orders" isSharded={false} totalCount={totalCount} />
        );

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();

        mockServices.databasesService
            .getMock("getDocumentsPreview")
            .mockImplementation(async (_databaseName, skip, take, collectionName) => ({
                items: DatabasesStubs.documentsPreviewItems(
                    skip,
                    Math.max(0, Math.min(take, totalCount - skip)),
                    collectionName
                ),
                totalResultCount: totalCount,
                // the documents deeper in the collection do not have the Freight property
                availableColumns: skip > 1000 ? ["Company", "__metadata"] : DatabasesStubs.documentsPreviewColumns(),
                continuationToken: null,
                resultEtag: "1",
            }));

        await openColumnSettings(screen);
        fireEvent.click(screen.getByRole("checkbox", { name: "Freight" }));
        fireEvent.click(screen.getByRole("button", { name: "Apply" }));
        await flushFetches();

        expect(await screen.findByText("Freight")).toBeInTheDocument();

        const scrollContainer = container.querySelector<HTMLDivElement>(".table-container");
        fireEvent.scroll(scrollContainer, { target: { scrollTop: 40_000 } });
        fireEvent.scroll(scrollContainer, { target: { scrollTop: 199_500 } });
        await flushFetches();

        expect(screen.getByText("Freight")).toBeInTheDocument();
    });

    it("shows the scroll to top button once the table is scrolled", async () => {
        const { screen, container } = rtlRender(
            <DocumentsListStory collection="Orders" isSharded={false} totalCount={1000} />
        );

        expect(await screen.findByText("orders/1-A")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Scroll to top" })).not.toBeInTheDocument();

        const scrollContainer = container.querySelector<HTMLDivElement>(".table-container");
        const scrollTo = jest.fn((options: ScrollToOptions) => {
            scrollContainer.scrollTop = options.top;
        });
        Object.defineProperty(scrollContainer, "scrollTo", { configurable: true, value: scrollTo });
        fireEvent.scroll(scrollContainer, { target: { scrollTop: 400 } });

        const scrollToTopButton = await screen.findByRole("button", { name: "Scroll to top" });
        fireEvent.click(scrollToTopButton);

        expect(scrollTo).toHaveBeenCalledWith({ top: 0, behavior: "smooth" });
        expect(scrollContainer.scrollTop).toBe(0);
        fireEvent.scroll(scrollContainer, { target: { scrollTop: 0 } });
        expect(screen.queryByRole("button", { name: "Scroll to top" })).not.toBeInTheDocument();
    });
});
