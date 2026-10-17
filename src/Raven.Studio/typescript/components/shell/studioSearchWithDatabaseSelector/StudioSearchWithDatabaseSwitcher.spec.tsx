import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import * as stories from "./StudioSearchWithDatabaseSwitcher.stories";
import { composeStories } from "@storybook/react-webpack5";
import { act } from "@testing-library/react";

const { DefaultStory } = composeStories(stories);

const searchPlaceholder = "Use Ctrl + K to search";

describe("StudioSearchWithDatabaseSwitcher", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultStory hasMenuItems={false} isDatabaseSelected={false} />);

        expect(await screen.findByPlaceholderText(searchPlaceholder)).toBeInTheDocument();
        expect(await screen.findByText("No database selected")).toBeInTheDocument();
    });

    it("does not list Buckets Report for a non-sharded database", async () => {
        const view = rtlRender(<DefaultStory isSharded={false} />);

        const resultTexts = await searchFor(view, "Report");

        expect(resultTexts).toContain("Storage Report");
        expect(resultTexts).not.toContain("Buckets Report");
    });

    it("lists Buckets Report for a sharded database", async () => {
        const view = rtlRender(<DefaultStory isSharded />);

        const resultTexts = await searchFor(view, "Report");

        expect(resultTexts).toContain("Buckets Report");
    });

    it("lists Buckets Report after switching from a non-sharded to a sharded database", async () => {
        const view = rtlRender(<DefaultStory isSharded={false} />);
        expect(await searchFor(view, "Report")).not.toContain("Buckets Report");

        view.rerender(<DefaultStory isSharded />);

        expect(await searchFor(view, "Report")).toContain("Buckets Report");
    });

    it("does not list Buckets Report after switching from a sharded to a non-sharded database", async () => {
        const view = rtlRender(<DefaultStory isSharded />);
        expect(await searchFor(view, "Report")).toContain("Buckets Report");

        view.rerender(<DefaultStory isSharded={false} />);

        expect(await searchFor(view, "Report")).not.toContain("Buckets Report");
    });
});

async function searchFor({ screen, user, fillInput }: ReturnType<typeof rtlRender>, query: string) {
    const input = await screen.findByPlaceholderText(searchPlaceholder);
    await act(() => user.click(input));
    await fillInput(input, query);

    return screen.getAllByRole("button").map((x) => x.textContent);
}
