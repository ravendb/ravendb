import React from "react";
import { RtlScreen, rtlRender, waitForElementToBeRemoved } from "test/rtlTestUtils";
import * as Stories from "./ConflictResolution.stories";
import { composeStories } from "@storybook/react";

const { DefaultConflictResolution } = composeStories(Stories);

const selectors = {
    names: {
        saveAll: /Save/,
        add: /Add new/,
        resolveUsingLatest: /If no script was defined for a collection, resolve the conflict using the latest version/,
    },
    titles: {
        saveEdit: /Save changes/,
        edit: /Edit this script/,
        delete: /Delete this script/,
        show: /Show this script/,
    },
};

describe("ConflictResolution", () => {
    const waitForLoad = async (screen: RtlScreen) => {
        await waitForElementToBeRemoved(screen.getByClassName("lazy-load"));
    };

    it("can render when database access is admin", async () => {
        const { screen } = rtlRender(<DefaultConflictResolution databaseAccess="DatabaseAdmin" />);
        await waitForLoad(screen);

        expect(screen.queryByRole("button", { name: selectors.names.saveAll })).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: selectors.names.add })).toBeInTheDocument();
        expect(screen.queryByRole("checkbox", { name: selectors.names.resolveUsingLatest })).toBeEnabled();
        expect(screen.queryAllByTitle(selectors.titles.edit)).toHaveLength(2);
        expect(screen.queryAllByTitle(selectors.titles.delete)).toHaveLength(2);
        expect(screen.queryAllByTitle(selectors.titles.show)).toHaveLength(0);
    });

    it("can render when database access below admin", async () => {
        const { screen } = rtlRender(<DefaultConflictResolution databaseAccess="DatabaseRead" />);
        await waitForLoad(screen);

        expect(screen.queryByRole("button", { name: selectors.names.saveAll })).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: selectors.names.add })).not.toBeInTheDocument();
        expect(screen.queryByRole("checkbox", { name: selectors.names.resolveUsingLatest })).toBeDisabled();
        expect(screen.queryAllByTitle(selectors.titles.edit)).toHaveLength(0);
        expect(screen.queryAllByTitle(selectors.titles.delete)).toHaveLength(0);
        expect(screen.queryAllByTitle(selectors.titles.show)).toHaveLength(2);
    });
});
