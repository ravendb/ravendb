import { rtlRender, fireEvent } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import * as Stories from "./PaginationBar.stories";

const { PaginationBar, PageNavigation, SimplePageNavigation } = composeStories(Stories);

describe("PaginationBar", () => {
    it("shows the range, rows per page and the page input", () => {
        const { screen } = rtlRender(<PaginationBar />);

        expect(screen.getByText("1-10 of 1,059")).toBeInTheDocument();
        expect(screen.getByRole("combobox", { name: "Rows per page" })).toBeInTheDocument();
        expect(screen.getByRole("spinbutton", { name: "Page" })).toHaveValue(1);

        fireEvent.click(screen.getByRole("button", { name: "Next page" }));

        expect(screen.getByText("11-20 of 1,059")).toBeInTheDocument();
    });
});

describe("PageNavigation", () => {
    it("disables going back on the first page and going forward on the last page", () => {
        const { screen } = rtlRender(<PageNavigation totalPages={3} />);

        expect(screen.getByRole("button", { name: "First page" })).toBeDisabled();
        expect(screen.getByRole("button", { name: "Previous page" })).toBeDisabled();

        fireEvent.click(screen.getByRole("button", { name: "Last page" }));

        expect(screen.getByRole("spinbutton", { name: "Page" })).toHaveValue(3);
        expect(screen.getByRole("button", { name: "Next page" })).toBeDisabled();
        expect(screen.getByRole("button", { name: "Last page" })).toBeDisabled();
    });

    it("always shows first and last page buttons in the standard variant", () => {
        const { screen } = rtlRender(<PageNavigation totalPages={2} />);

        expect(screen.getByRole("button", { name: "First page" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Last page" })).toBeInTheDocument();
    });

    it("shows the page as text without an input in the simple variant", () => {
        const { screen } = rtlRender(<SimplePageNavigation totalPages={12} />);

        expect(screen.getByText("Page 1 of 12")).toBeInTheDocument();
        expect(screen.queryByRole("spinbutton", { name: "Page" })).not.toBeInTheDocument();

        fireEvent.click(screen.getByRole("button", { name: "Next page" }));

        expect(screen.getByText("Page 2 of 12")).toBeInTheDocument();
    });

    it("hides first and last page buttons in the simple variant when there are fewer than 5 pages", () => {
        const { screen } = rtlRender(<SimplePageNavigation totalPages={4} />);

        expect(screen.queryByRole("button", { name: "First page" })).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: "Last page" })).not.toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Previous page" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Next page" })).toBeInTheDocument();
    });

    it("shows first and last page buttons in the simple variant from 5 pages", () => {
        const { screen } = rtlRender(<SimplePageNavigation totalPages={5} />);

        expect(screen.getByRole("button", { name: "First page" })).toBeInTheDocument();
        expect(screen.getByRole("button", { name: "Last page" })).toBeInTheDocument();
    });
});
