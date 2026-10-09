import { rtlRender } from "test/rtlTestUtils";
import React from "react";
import * as stories from "./StudioSearchWithDatabaseSwitcher.stories";
import { composeStories } from "@storybook/react-webpack5";
import { act } from "@testing-library/react";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultStory } = composeStories(stories);

const searchPlaceholder = "Use Ctrl + K to search";

describe("StudioSearchWithDatabaseSwitcher", function () {
    afterEach(() => {
        activeDatabaseTracker.default.database(null);
    });

    it("can render", async () => {
        const { screen } = rtlRender(<DefaultStory hasMenuItems={false} isDatabaseSelected={false} />);

        expect(await screen.findByPlaceholderText(searchPlaceholder)).toBeInTheDocument();
        expect(await screen.findByText("No database selected")).toBeInTheDocument();
    });

    it("does not list Buckets Report for a non-sharded database", async () => {
        activeDatabaseTracker.default.database(DatabasesStubs.nonShardedSingleNodeDatabase());

        const resultTexts = await searchFor("Report");

        expect(resultTexts).toContain("Storage Report");
        expect(resultTexts).not.toContain("Buckets Report");
    });

    it("lists Buckets Report for a sharded database", async () => {
        activeDatabaseTracker.default.database(DatabasesStubs.shardedDatabase());

        const resultTexts = await searchFor("Report");

        expect(resultTexts).toContain("Buckets Report");
    });
});

async function searchFor(query: string) {
    const { screen, user, fillInput } = rtlRender(<DefaultStory />);

    const input = await screen.findByPlaceholderText(searchPlaceholder);
    await act(() => user.click(input));
    await fillInput(input, query);

    return screen.getAllByRole("button").map((x) => x.textContent);
}
