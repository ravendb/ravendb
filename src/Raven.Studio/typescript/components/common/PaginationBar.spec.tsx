import { rtlRender, fireEvent } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import * as PaginationBarStories from "./PaginationBar.stories";
import * as PageNavigationStories from "./PageNavigation.stories";

const PaginationBar = composeStories(PaginationBarStories);
const PageNavigation = composeStories(PageNavigationStories);

describe("PaginationBar", () => {
    it("shows the range, items per page and the page input", () => {
        const { screen } = rtlRender(<PaginationBar.Default />);

        expect(screen.getByText("1-10 of 1,059")).toBeInTheDocument();
        expect(screen.getByRole("combobox", { name: "Items per page" })).toBeInTheDocument();
        expect(screen.getByRole("spinbutton", { name: "Page" })).toHaveValue(1);

        fireEvent.click(screen.getByRole("button", { name: "Next page" }));

        expect(screen.getByText("11-20 of 1,059")).toBeInTheDocument();
    });

    it("shows no items and no page buttons when empty", () => {
        const { screen } = rtlRender(<PaginationBar.Empty />);

        expect(screen.getByText("No items")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Next page" })).not.toBeInTheDocument();
    });

    it("shows the range without a total when the total is unknown", () => {
        const { screen } = rtlRender(<PaginationBar.UnknownTotal />);

        expect(screen.getByText("1-10")).toBeInTheDocument();
    });
});

describe("PageNavigation", () => {
    it("disables going back on the first page and going forward on the last page", () => {
        const { screen } = rtlRender(<PageNavigation.Default totalPages={3} />);

        expect(screen.getByRole("button", { name: "First page" })).toBeDisabled();
        expect(screen.getByRole("button", { name: "Previous page" })).toBeDisabled();

        fireEvent.click(screen.getByRole("button", { name: "Last page" }));

        expect(screen.getByRole("spinbutton", { name: "Page" })).toHaveValue(3);
        expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
        expect(screen.getByRole("button", { name: "Last page" })).toBeDisabled();
    });

    it("always shows first and last page buttons in the standard variant", () => {
        const { screen } = rtlRender(<PageNavigation.Default totalPages={2} />);

        expect(screen.getByRole("button", { name: "First page" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Last page" })).toBeInTheDocument();
    });

    it("shows the page as text without an input in the simple variant", () => {
        const { screen } = rtlRender(<PageNavigation.Simple />);

        expect(screen.getByText("Page 1 of 12")).toBeInTheDocument();
        expect(screen.queryByRole("spinbutton", { name: "Page" })).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole("button", { name: "Next page" }));

        expect(screen.getByText("Page 2 of 12")).toBeInTheDocument();
    });

    it("hides first and last page buttons in the simple variant when there are fewer than 5 pages", () => {
        const { screen } = rtlRender(<PageNavigation.SimpleFewPages />);

        expect(screen.queryByRole("button", { name: "First page" })).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Last page" })).not.toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Previous page" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Next page" })).toBeInTheDocument();
    });

    it("shows first and last page buttons in the simple variant from 5 pages", () => {
        const { screen } = rtlRender(<PageNavigation.Simple totalPages={5} />);

        expect(screen.getByRole("button", { name: "First page" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Last page" })).toBeInTheDocument();
    });

    it("is hidden when there is only one page", () => {
        const { screen } = rtlRender(<PageNavigation.SinglePage />);

        expect(screen.queryByRole("button", { name: "Next page" })).not.toBeInTheDocument();
        expect(screen.queryByRole("spinbutton", { name: "Page" })).not.toBeInTheDocument();
    });
});
