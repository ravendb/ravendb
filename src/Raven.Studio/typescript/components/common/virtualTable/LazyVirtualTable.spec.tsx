import { rtlRender, fireEvent } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import * as Stories from "./VirtualTable.stories";
import { virtualTableConstants } from "./utils/virtualTableConstants";

const { LazyVirtualTableStory } = composeStories(Stories);

const { defaultRowHeightInPx, maxBodyHeightInPx, headerHeightInPx, paddingInPx } = virtualTableConstants;
const MAX_ROWS_IN_DOM = maxBodyHeightInPx / defaultRowHeightInPx;
const TOTAL_COUNT = 100_000_001;
// picked so that the fitting page size divides MAX_ROWS_IN_DOM
const TABLE_HEIGHT_IN_PX = 480;
const PAGE_SIZE = Math.floor((TABLE_HEIGHT_IN_PX - paddingInPx - headerHeightInPx) / defaultRowHeightInPx);
const VIEWPORT_HEIGHT_IN_PX = 400;

function getScrollContainer(container: HTMLElement) {
    return container.querySelector<HTMLDivElement>(".table-container");
}

// jsdom has no layout, so the scroll geometry is derived from the rendered body height
function mockLayout(scrollContainer: HTMLDivElement, viewportHeightInPx = VIEWPORT_HEIGHT_IN_PX) {
    Object.defineProperty(scrollContainer, "clientHeight", { configurable: true, value: viewportHeightInPx });
    Object.defineProperty(scrollContainer, "scrollHeight", {
        configurable: true,
        get: () => parseFloat(scrollContainer.querySelector("tbody").style.height) + headerHeightInPx,
    });
}

function scrollTo(scrollContainer: HTMLDivElement, scrollTop: number) {
    fireEvent.scroll(scrollContainer, { target: { scrollTop } });
}

describe("LazyVirtualTable", () => {
    describe.each(["skipTake", "continuationToken"] as const)("%s mode", (fetchMode) => {
        it("renders rows fetched for the visible range", async () => {
            const { screen } = rtlRender(
                <LazyVirtualTableStory totalCount={TOTAL_COUNT} fetchMode={fetchMode} fetchDelayInMs={0} />
            );

            expect(await screen.findByText("Item 0")).toBeInTheDocument();
            expect(screen.getByText("Item 19")).toBeInTheDocument();
            expect(screen.queryByText("Item 500")).not.toBeInTheDocument();
            expect(screen.queryByTestId("dom-limit-banner")).not.toBeInTheDocument();
        });
    });

    it("shows the end of DOM banner at the bottom and switches to the last page of the DOM", async () => {
        const { screen, container } = rtlRender(
            <LazyVirtualTableStory
                totalCount={TOTAL_COUNT}
                fetchMode="skipTake"
                fetchDelayInMs={0}
                minFetchCount={PAGE_SIZE}
                heightInPx={TABLE_HEIGHT_IN_PX}
            />
        );

        expect(await screen.findByText("Item 0")).toBeInTheDocument();

        const scrollContainer = getScrollContainer(container);
        const viewportHeightInPx = (PAGE_SIZE + 5) * defaultRowHeightInPx;
        mockLayout(scrollContainer, viewportHeightInPx);

        scrollTo(scrollContainer, maxBodyHeightInPx + headerHeightInPx - viewportHeightInPx);

        expect(await screen.findByTestId("dom-limit-banner")).toBeInTheDocument();
        expect(await screen.findByText(`Item ${MAX_ROWS_IN_DOM - 1}`)).toBeInTheDocument();
        expect(screen.queryByText(`Item ${MAX_ROWS_IN_DOM}`)).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole("button", { name: "Turn on pagination" }));

        const firstRowOfPage = await screen.findByText(`Item ${MAX_ROWS_IN_DOM - PAGE_SIZE}`);
        expect(firstRowOfPage.closest("tr")).toHaveStyle({ transform: "translateY(0px)" });
        expect(scrollContainer.querySelector("tbody")).toHaveStyle({
            height: `${PAGE_SIZE * defaultRowHeightInPx}px`,
        });
        expect(screen.queryByTestId("dom-limit-banner")).not.toBeInTheDocument();
        expect(
            screen.getByText(
                `${(MAX_ROWS_IN_DOM - PAGE_SIZE + 1).toLocaleString()}-${MAX_ROWS_IN_DOM.toLocaleString()} of ${TOTAL_COUNT.toLocaleString()}`
            )
        ).toBeInTheDocument();

        const expectedPage = MAX_ROWS_IN_DOM / PAGE_SIZE;
        expect(screen.getByRole("spinbutton", { name: "Page" })).toHaveValue(expectedPage);
    });

    it("suggests a query instead of pagination at the end of DOM in continuation token mode", async () => {
        const minFetchCount = MAX_ROWS_IN_DOM / 4;

        const { screen, container } = rtlRender(
            <LazyVirtualTableStory
                totalCount={TOTAL_COUNT}
                fetchMode="continuationToken"
                fetchDelayInMs={0}
                minFetchCount={minFetchCount}
                heightInPx={TABLE_HEIGHT_IN_PX}
            />
        );

        expect(await screen.findByText("Item 0")).toBeInTheDocument();

        const scrollContainer = getScrollContainer(container);
        mockLayout(scrollContainer);

        for (let loaded = minFetchCount; loaded < MAX_ROWS_IN_DOM; loaded += minFetchCount) {
            scrollTo(scrollContainer, loaded * defaultRowHeightInPx);
            expect(await screen.findByText(`Item ${loaded}`)).toBeInTheDocument();
        }

        scrollTo(scrollContainer, maxBodyHeightInPx);

        expect(await screen.findByTestId("dom-limit-banner")).toHaveTextContent("Use a query to find more items");
        expect(screen.queryByRole("button", { name: "Turn on pagination" })).not.toBeInTheDocument();
    });

    it("can toggle pagination with the switch and fits the page into the table height", async () => {
        const { screen, container } = rtlRender(
            <LazyVirtualTableStory
                totalCount={TOTAL_COUNT}
                fetchMode="skipTake"
                fetchDelayInMs={0}
                heightInPx={TABLE_HEIGHT_IN_PX}
            />
        );

        expect(await screen.findByText("Item 0")).toBeInTheDocument();

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText(`1-${PAGE_SIZE} of ${TOTAL_COUNT.toLocaleString()}`)).toBeInTheDocument();
        expect(getScrollContainer(container).querySelector("tbody")).toHaveStyle({
            height: `${PAGE_SIZE * defaultRowHeightInPx}px`,
        });
        expect(screen.queryByText(`Item ${PAGE_SIZE}`)).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText(`Item ${PAGE_SIZE}`)).toBeInTheDocument();
    });

    it("switches to the page starting at the scrolled row when the browser reports a fractional scroll position", async () => {
        const { screen, container } = rtlRender(
            <LazyVirtualTableStory
                totalCount={TOTAL_COUNT}
                fetchMode="skipTake"
                fetchDelayInMs={0}
                heightInPx={TABLE_HEIGHT_IN_PX}
            />
        );

        expect(await screen.findByText("Item 0")).toBeInTheDocument();

        const scrollContainer = getScrollContainer(container);
        mockLayout(scrollContainer);

        const pageNumber = 5;
        scrollTo(scrollContainer, (pageNumber - 1) * PAGE_SIZE * defaultRowHeightInPx - 0.8);

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByRole("spinbutton", { name: "Page" })).toHaveValue(pageNumber);
        expect(await screen.findByText(`Item ${(pageNumber - 1) * PAGE_SIZE}`)).toBeInTheDocument();
    });

    it("can pick the rows per page and jump to a page from the input", async () => {
        const { screen, container } = rtlRender(
            <LazyVirtualTableStory
                totalCount={TOTAL_COUNT}
                fetchMode="skipTake"
                fetchDelayInMs={0}
                heightInPx={TABLE_HEIGHT_IN_PX}
            />
        );

        expect(await screen.findByText("Item 0")).toBeInTheDocument();

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText(`1-${PAGE_SIZE} of ${TOTAL_COUNT.toLocaleString()}`)).toBeInTheDocument();
        expect(screen.getByText(String(PAGE_SIZE))).toBeInTheDocument();

        fireEvent.keyDown(screen.getByRole("combobox", { name: "Rows per page" }), { key: "ArrowDown" });
        fireEvent.click(await screen.findByText("100"));

        expect(await screen.findByText("Item 99")).toBeInTheDocument();
        expect(screen.getByText(`1-100 of ${TOTAL_COUNT.toLocaleString()}`)).toBeInTheDocument();
        expect(getScrollContainer(container).querySelector("tbody")).toHaveStyle({
            height: `${100 * defaultRowHeightInPx}px`,
        });

        const pageInput = screen.getByRole("spinbutton", { name: "Page" });
        fireEvent.change(pageInput, { target: { value: "3" } });

        expect(await screen.findByText("Item 200")).toBeInTheDocument();
        expect(screen.getByText(`201-300 of ${TOTAL_COUNT.toLocaleString()}`)).toBeInTheDocument();

        fireEvent.click(screen.getByRole("button", { name: "Last page" }));

        const totalPages = Math.ceil(TOTAL_COUNT / 100);
        expect(await screen.findByText(`Item ${TOTAL_COUNT - 1}`)).toBeInTheDocument();
        expect(pageInput).toHaveValue(totalPages);
        expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
    });

    it("ignores an out of range page typed into the input and restores the current page on blur", async () => {
        const { screen } = rtlRender(
            <LazyVirtualTableStory
                totalCount={1000}
                fetchMode="skipTake"
                fetchDelayInMs={0}
                heightInPx={TABLE_HEIGHT_IN_PX}
            />
        );

        expect(await screen.findByText("Item 0")).toBeInTheDocument();

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        const pageInput = await screen.findByRole("spinbutton", { name: "Page" });

        fireEvent.change(pageInput, { target: { value: "4" } });

        const firstRowOfPage = `Item ${3 * PAGE_SIZE}`;
        expect(await screen.findByText(firstRowOfPage)).toBeInTheDocument();

        fireEvent.change(pageInput, { target: { value: "45000" } });

        expect(pageInput).toHaveValue(45000);
        expect(screen.getByText(firstRowOfPage)).toBeInTheDocument();

        fireEvent.blur(pageInput);

        expect(pageInput).toHaveValue(4);
        expect(screen.getByText(firstRowOfPage)).toBeInTheDocument();
    });
});
