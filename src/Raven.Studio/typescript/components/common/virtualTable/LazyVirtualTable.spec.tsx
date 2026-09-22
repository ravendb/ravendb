import { rtlRender, fireEvent } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import * as Stories from "./VirtualTable.stories";
import { virtualTableConstants } from "./utils/virtualTableConstants";

const { LazyVirtualTableStory } = composeStories(Stories);

const { defaultRowHeightInPx, maxBodyHeightInPx, headerHeightInPx, paddingInPx } = virtualTableConstants;
const maxRowsInDom = maxBodyHeightInPx / defaultRowHeightInPx;
const totalCount = 100_000_001;
// picked so that the fitting page size divides maxRowsInDom
const tableHeightInPx = 480;
// rows that fit into the table when paginated
const pageSize = Math.floor((tableHeightInPx - paddingInPx - headerHeightInPx) / defaultRowHeightInPx);
const viewportHeightInPx = 400;

function getScrollContainer(container: HTMLElement) {
    return container.querySelector<HTMLDivElement>(".table-container");
}

// jsdom has no layout, so the scroll geometry is derived from the rendered body height
function mockLayout(scrollContainer: HTMLDivElement) {
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
                <LazyVirtualTableStory totalCount={totalCount} fetchMode={fetchMode} fetchDelayInMs={0} />
            );

            expect(await screen.findByText("Item 0")).toBeInTheDocument();
            expect(screen.getByText("Item 19")).toBeInTheDocument();
            expect(screen.queryByText("Item 500")).not.toBeInTheDocument();
            expect(screen.queryByTestId("dom-limit-banner")).not.toBeInTheDocument();
        });

        it("shows the end of DOM banner at the bottom and switches to the page matching the scroll position", async () => {
            const minFetchCount = fetchMode === "continuationToken" ? maxRowsInDom / 4 : pageSize;

            const { screen, container } = rtlRender(
                <LazyVirtualTableStory
                    totalCount={totalCount}
                    fetchMode={fetchMode}
                    fetchDelayInMs={0}
                    minFetchCount={minFetchCount}
                    heightInPx={tableHeightInPx}
                />
            );

            expect(await screen.findByText("Item 0")).toBeInTheDocument();

            const scrollContainer = getScrollContainer(container);
            mockLayout(scrollContainer);

            if (fetchMode === "continuationToken") {
                for (let loaded = minFetchCount; loaded < maxRowsInDom; loaded += minFetchCount) {
                    scrollTo(scrollContainer, loaded * defaultRowHeightInPx);
                    expect(await screen.findByText(`Item ${loaded}`)).toBeInTheDocument();
                }
            }

            scrollTo(scrollContainer, maxBodyHeightInPx);

            expect(await screen.findByTestId("dom-limit-banner")).toBeInTheDocument();
            expect(await screen.findByText(`Item ${maxRowsInDom - 1}`)).toBeInTheDocument();
            expect(screen.queryByText(`Item ${maxRowsInDom}`)).not.toBeInTheDocument();

            fireEvent.click(screen.getByRole("button", { name: "Turn on pagination" }));

            const firstRowOfPage = await screen.findByText(`Item ${maxRowsInDom}`);
            expect(firstRowOfPage.closest("tr")).toHaveStyle({ transform: "translateY(0px)" });
            expect(scrollContainer.querySelector("tbody")).toHaveStyle({
                height: `${pageSize * defaultRowHeightInPx}px`,
            });
            expect(screen.queryByTestId("dom-limit-banner")).not.toBeInTheDocument();
            expect(
                screen.getByText(
                    `${(maxRowsInDom + 1).toLocaleString()}-${(maxRowsInDom + pageSize).toLocaleString()} of ${totalCount.toLocaleString()}`
                )
            ).toBeInTheDocument();

            const expectedPage = maxRowsInDom / pageSize + 1;
            expect(screen.getByRole("textbox", { name: "Page" })).toHaveValue(String(expectedPage));

            fireEvent.click(screen.getByRole("button", { name: "Turn off pagination" }));

            expect(await screen.findByText(`Item ${maxRowsInDom - 1}`)).toBeInTheDocument();
            expect(screen.queryByText("Turn off pagination")).not.toBeInTheDocument();
        });
    });

    it("can toggle pagination with the switch and fits the page into the table height", async () => {
        const { screen, container } = rtlRender(
            <LazyVirtualTableStory
                totalCount={totalCount}
                fetchMode="skipTake"
                fetchDelayInMs={0}
                heightInPx={tableHeightInPx}
            />
        );

        expect(await screen.findByText("Item 0")).toBeInTheDocument();

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText(`1-${pageSize} of ${totalCount.toLocaleString()}`)).toBeInTheDocument();
        expect(getScrollContainer(container).querySelector("tbody")).toHaveStyle({
            height: `${pageSize * defaultRowHeightInPx}px`,
        });
        expect(screen.queryByText(`Item ${pageSize}`)).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText(`Item ${pageSize}`)).toBeInTheDocument();
        expect(screen.queryByText("Turn off pagination")).not.toBeInTheDocument();
    });

    it("can pick the rows per page and jump to a page from the input", async () => {
        const { screen, container } = rtlRender(
            <LazyVirtualTableStory
                totalCount={totalCount}
                fetchMode="skipTake"
                fetchDelayInMs={0}
                heightInPx={tableHeightInPx}
            />
        );

        expect(await screen.findByText("Item 0")).toBeInTheDocument();

        fireEvent.click(screen.getByRole("checkbox", { name: "Pagination" }));

        expect(await screen.findByText(`1-${pageSize} of ${totalCount.toLocaleString()}`)).toBeInTheDocument();
        expect(screen.getByText(String(pageSize))).toBeInTheDocument();

        fireEvent.keyDown(screen.getByRole("combobox", { name: "Rows per page" }), { key: "ArrowDown" });
        fireEvent.click(await screen.findByText("100"));

        expect(await screen.findByText("Item 99")).toBeInTheDocument();
        expect(screen.getByText(`1-100 of ${totalCount.toLocaleString()}`)).toBeInTheDocument();
        expect(getScrollContainer(container).querySelector("tbody")).toHaveStyle({
            height: `${100 * defaultRowHeightInPx}px`,
        });

        const pageInput = screen.getByRole("textbox", { name: "Page" });
        fireEvent.change(pageInput, { target: { value: "3" } });
        fireEvent.keyDown(pageInput, { key: "Enter" });

        expect(await screen.findByText("Item 200")).toBeInTheDocument();
        expect(screen.getByText(`201-300 of ${totalCount.toLocaleString()}`)).toBeInTheDocument();

        fireEvent.click(screen.getByRole("button", { name: "Last page" }));

        const totalPages = Math.ceil(totalCount / 100);
        expect(await screen.findByText(`Item ${totalCount - 1}`)).toBeInTheDocument();
        expect(pageInput).toHaveValue(String(totalPages));
        expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
    });
});
