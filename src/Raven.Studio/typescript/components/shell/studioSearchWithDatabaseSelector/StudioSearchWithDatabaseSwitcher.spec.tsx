import { act, rtlRender, waitFor } from "test/rtlTestUtils";
import React from "react";
import * as stories from "./StudioSearchWithDatabaseSwitcher.stories";
import { composeStories } from "@storybook/react-webpack5";
import { mockServices } from "test/mocks/services/MockServices";
import { globalDispatch } from "components/storeCompat";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import generateMenuItems from "common/shell/menu/generateMenuItems";

const { DefaultStory } = composeStories(stories);

const searchPlaceholder = "Use Ctrl + K to search";

describe("StudioSearchWithDatabaseSwitcher", function () {
    beforeAll(loadMenuItemModules);

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

    it("does not refetch tasks and indexes when unchanged databases are reloaded", async () => {
        const { getOngoingTasks } = mockServices.tasksService.mock;
        const { getStats } = mockServices.indexesService.mock;

        rtlRender(<DefaultStory />);
        await waitFor(() => expect(getOngoingTasks).toHaveBeenCalled());

        const tasksCallsCount = (getOngoingTasks as jest.Mock).mock.calls.length;
        const statsCallsCount = (getStats as jest.Mock).mock.calls.length;

        act(() =>
            globalDispatch((dispatch, getState) =>
                dispatch(
                    databaseActions.databasesLoaded(databaseSelectors.allDatabases(getState()).map((db) => ({ ...db })))
                )
            )
        );

        expect(getOngoingTasks).toHaveBeenCalledTimes(tasksCallsCount);
        expect(getStats).toHaveBeenCalledTimes(statsCallsCount);
    });
});

function loadMenuItemModules() {
    generateMenuItems({ db: "db", isSharded: true, isWhatsNewVisible: false });
}

async function searchFor({ screen, user, fillInput }: ReturnType<typeof rtlRender>, query: string) {
    const input = await screen.findByPlaceholderText(searchPlaceholder);
    await user.click(input);
    await fillInput(input, query);

    return screen.getAllByRole("button").map((x) => x.textContent);
}
