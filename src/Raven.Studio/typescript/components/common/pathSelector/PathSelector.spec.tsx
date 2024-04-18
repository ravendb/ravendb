import { composeStories } from "@storybook/react";
import { exportedForTesting } from "./PathSelector";
import * as stories from "./PathSelector.stories";
import { rtlRender } from "test/rtlTestUtils";
import React from "react";

const { getParentPath, formatPathInList } = exportedForTesting;
const { PathSelectorStory } = composeStories(stories);

const selectors = {
    mainButtonText: /Select path/,
    goBackButtonTitle: /Go back/,
};

describe("PathSelector", () => {
    describe("getParentPath", () => {
        it.each([
            ["/path/to/dir/", true, "/path/to/"],
            ["/path/to/dir", true, "/path/to/"],
            ["/path/to", true, "/path/"],
            ["/path", true, ""],
            ["", false, ""],
            ["C:\\path\\to\\dir\\", true, "C:\\path\\to\\"],
            ["C:\\path\\to\\dir", true, "C:\\path\\to\\"],
            ["C:\\path\\to", true, "C:\\path\\"],
            ["C:\\path", true, "C:\\"],
            ["C:\\", true, ""],
            ["", false, ""],
        ])("for %p returns canGoBack %p, and parent path %p ", (path, expectedCanGoBack, expectedParentDir) => {
            const result = getParentPath(path);

            expect(result.canGoBack).toBe(expectedCanGoBack);
            expect(result.parentDir).toBe(expectedParentDir);
        });
    });

    describe("formatPathInList", () => {
        it.each([
            ["C:\\", "", "C:\\"],
            ["C:\\", "C", "C:\\"],
            ["C:\\path", "C:\\", "path"],
            ["C:\\path\\to", "C:\\path\\", "to"],
            ["/", "", "/"],
            ["/path", "/", "path"],
            ["/path/to", "/path/", "to"],
            ["/path/to/dir", "/path/to/", "dir"],
        ])("for list item %p and path %p returns %p", (listItemPath, pathInput, expected) => {
            const result = formatPathInList(listItemPath, pathInput);

            expect(result).toBe(expected);
        });
    });

    describe("PathSelector", () => {
        it("can render empty list", async () => {
            const { screen, fireClick, waitForLoad } = rtlRender(<PathSelectorStory paths={[]} />);

            await fireClick(screen.getByRole("button", { name: selectors.mainButtonText }));
            await waitForLoad();

            expect(screen.getByText(/No results found/)).toBeInTheDocument();
        });

        it("can render error", async () => {
            const { screen, fireClick, waitForLoad } = rtlRender(<PathSelectorStory isErrored />);

            await fireClick(screen.getByRole("button", { name: selectors.mainButtonText }));
            await waitForLoad();

            expect(screen.getByText(/Failed to load paths/)).toBeInTheDocument();
        });

        it("can go to directory and go back", async () => {
            const { screen, fireClick, waitForLoad } = rtlRender(
                <PathSelectorStory defaultPath="C:\" paths={["C:\\Desktop"]} />
            );

            await fireClick(screen.getByRole("button", { name: selectors.mainButtonText }));
            await waitForLoad();
            expect(screen.getByRole("textbox", { name: "Path" })).toHaveValue("C:\\");

            await fireClick(screen.getByText("Desktop"));
            await waitForLoad();
            expect(screen.getByRole("textbox", { name: "Path" })).toHaveValue("C:\\Desktop\\");

            await fireClick(screen.getByTitle(selectors.goBackButtonTitle));
            expect(screen.getByRole("textbox", { name: "Path" })).toHaveValue("C:\\");
        });
    });
});
