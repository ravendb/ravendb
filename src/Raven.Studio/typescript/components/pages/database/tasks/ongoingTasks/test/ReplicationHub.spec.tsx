import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/stories/ReplicationHub.stories";
import { rtlRender } from "test/rtlTestUtils";
import { within } from "@testing-library/dom";

import React from "react";

const containerTestId = "replications";

describe("Replication Hub", function () {
    it("can render hub w/o connections", async () => {
        const Story = composeStory(stories.NoConnections, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByRole("heading", { name: "Replication", level: 5 })).toBeInTheDocument();
        expect(await container.findByText(/Replication Hub/)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/, { selector: "button" })).toBeInTheDocument();
        expect(container.queryByText(/Disabled/, { selector: "button" })).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByText(/No sinks connected/)).toBeInTheDocument();
    });

    it("can render hub w/ connections", async () => {
        const Story = composeStory(stories.Default, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));
        expect(await container.findByRole("heading", { name: "Replication", level: 5 })).toBeInTheDocument();
        expect(await container.findByText(/Replication Hub/)).toBeInTheDocument();
        expect(await container.findByText(/Enabled/, { selector: "button" })).toBeInTheDocument();
        expect(container.queryByText(/Disabled/)).not.toBeInTheDocument();

        const detailsBtn = await container.findByTitle(/Click for details/);

        await fireClick(detailsBtn);

        expect(await container.findByRole("heading", { name: "target-hub-db" })).toBeInTheDocument();
        expect(await container.findByText(/Actual Sink URL/)).toBeInTheDocument();

        await fireClick(await container.findByTitle(/Expand distribution details/));

        expect(await container.findByTitle(/Collapse distribution details/)).toBeInTheDocument();
        expect(await container.findByText(/Last DB Etag/)).toBeInTheDocument();
        expect(await container.findByText(/Last Sent Etag/)).toBeInTheDocument();
    });

    it("can filter, sort and expand all sinks", async () => {
        const Story = composeStory(stories.ManySinks, stories.default);

        const { screen, fireClick, user } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        await fireClick(await container.findByTitle(/Click for details/));

        expect(await container.findByText("Replication sinks")).toBeInTheDocument();
        expect(container.getAllByTitle(/^Expand distribution details for /)).toHaveLength(15);

        const filterToggle = container.getByTitle("Filter sinks");
        const filterDropdown = within(filterToggle.parentElement);
        await fireClick(filterToggle);
        await fireClick(await filterDropdown.findByRole("button", { name: /Sink database name/ }));
        await user.type(await filterDropdown.findByPlaceholderText("e.g. name"), "berlin");

        expect(container.getAllByTitle(/^Expand distribution details for /)).toHaveLength(1);
        expect(container.getByTitle("Expand distribution details for Sink-Berlin")).toBeInTheDocument();
        expect(container.getByText("Sink-Berlin")).toBeInTheDocument();
        expect(filterToggle).toHaveTextContent("1");
        expect(filterToggle).toHaveAccessibleName("Filter sinks (1 active)");

        await fireClick(filterDropdown.getByTitle("Back to filters"));

        expect(filterDropdown.getByRole("button", { name: "Sink database name (active)" })).toBeInTheDocument();
        expect(filterDropdown.getByRole("button", { name: "Replication state" })).toBeInTheDocument();
        expect(container.getByTitle(/Change "Sink database name" filter/)).toBeInTheDocument();
        expect(
            container.getByText(
                (_, element) =>
                    element?.classList.contains("sinks-results-count") && element.textContent === "1 of 15 sinks"
            )
        ).toBeInTheDocument();
        expect(container.getByText("1 of 15 sinks shown")).toBeInTheDocument();

        await fireClick(container.getByTitle('Clear "Sink database name" filter'));

        expect(container.getAllByTitle(/^Expand distribution details for /)).toHaveLength(15);
        expect(container.queryByTitle(/Change "Sink database name" filter/)).not.toBeInTheDocument();
        expect(container.queryByText(/of 15 sinks/)).not.toBeInTheDocument();
        expect(filterToggle).not.toHaveTextContent(/\d/);
        expect(filterToggle).toHaveAccessibleName("Filter sinks");

        await fireClick(filterToggle);
        await fireClick(await filterDropdown.findByRole("button", { name: /Sink database name/ }));
        await user.type(await filterDropdown.findByPlaceholderText("e.g. name"), "no-such-sink");

        expect(container.queryAllByTitle(/^Expand distribution details for /)).toHaveLength(0);
        expect(container.getByText("No sinks match the selected filters")).toBeInTheDocument();

        await fireClick(container.getByTitle('Clear "Sink database name" filter'));

        expect(container.getAllByTitle(/^Expand distribution details for /)).toHaveLength(15);

        const getSinkDatabases = () => container.getAllByText(/^Sink-/).map((x) => x.textContent);

        const sortToggle = container.getByTitle("Sorted by Sink database name, ascending");
        const sortDropdown = within(sortToggle.parentElement);

        expect(sortToggle).toHaveTextContent("Sort by:Sink database name");
        expect(getSinkDatabases()[0]).toBe("Sink-Berlin");
        expect(getSinkDatabases().at(-1)).toBe("Sink-Warsaw");

        await fireClick(sortToggle);
        await user.click(sortDropdown.getByText("Ascending"));
        await user.click(await sortDropdown.findByRole("option", { name: "Descending" }));

        expect(sortToggle).toHaveAttribute("title", "Sorted by Sink database name, descending");
        expect(getSinkDatabases()[0]).toBe("Sink-Warsaw");
        expect(getSinkDatabases().at(-1)).toBe("Sink-Berlin");

        await user.click(sortDropdown.getByLabelText("Sort by"));
        await user.click(await sortDropdown.findByRole("option", { name: "Replication status" }));

        expect(sortToggle).toHaveTextContent("Sort by:Replication status");
        expect(getSinkDatabases()[0]).toBe("Sink-Berlin");
        expect(getSinkDatabases().at(-1)).toBe("Sink-Singapore");

        await user.click(sortDropdown.getByText("Descending"));
        await user.click(await sortDropdown.findByRole("option", { name: "Ascending" }));

        expect(getSinkDatabases().slice(0, 3)).toEqual([
            "Sink-SanFrancisco-Regional-Warehouse",
            "Sink-SaoPaulo",
            "Sink-Singapore",
        ]);
        expect(getSinkDatabases().at(-1)).toBe("Sink-Sydney");

        await fireClick(container.getByTitle("Expand all sinks"));

        expect(container.getAllByTitle(/^Collapse distribution details for /)).toHaveLength(15);
        expect(container.getByTitle("Collapse all sinks")).toBeInTheDocument();
    });

    it("filters sinks by the replication states shown on the sink cards", async () => {
        const Story = composeStory(stories.ManySinks, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        await fireClick(await container.findByTitle(/Click for details/));
        await fireClick(container.getByTitle("Expand all sinks"));

        expect(await container.findAllByText("up to date")).toHaveLength(8);
        expect(container.getAllByText("?")).toHaveLength(7);

        const filterToggle = container.getByTitle("Filter sinks");
        const filterDropdown = within(filterToggle.parentElement);
        await fireClick(filterToggle);
        await fireClick(await filterDropdown.findByRole("button", { name: /Replication state/ }));

        for (const state of ["Up to date", "Running", "Disabled", "Unknown"]) {
            expect(filterDropdown.getByLabelText(state)).toBeInTheDocument();
        }
        expect(filterDropdown.queryByLabelText("Success")).not.toBeInTheDocument();

        await fireClick(filterDropdown.getByLabelText("Unknown"));

        expect(container.getAllByTitle(/^Collapse distribution details for /)).toHaveLength(7);
        expect(container.getAllByText("?")).toHaveLength(7);
        expect(container.queryByText("up to date")).not.toBeInTheDocument();
        expect(container.getByTitle(/Change "Replication state" filter/)).toHaveTextContent(
            /Replication state:\s*Unknown/
        );
    });

    it("keeps keyboard focus inside the filter menus", async () => {
        const Story = composeStory(stories.ManySinks, stories.default);

        const { screen, fireClick, user } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        await fireClick(await container.findByTitle(/Click for details/));

        const filterToggle = container.getByTitle("Filter sinks");
        const filterDropdown = within(filterToggle.parentElement);
        await fireClick(filterToggle);
        await fireClick(await filterDropdown.findByRole("button", { name: /Replication state/ }));

        expect(filterDropdown.getByRole("group", { name: "Replication state" })).toBeInTheDocument();
        expect(filterDropdown.getByLabelText("Up to date")).toHaveFocus();

        await user.keyboard(" ");

        expect(container.getByTitle(/Change "Replication state" filter/)).toHaveTextContent(
            /Replication state:\s*Up to date/
        );

        await fireClick(filterDropdown.getByTitle("Back to filters"));

        expect(filterDropdown.getByRole("button", { name: /Replication state/ })).toHaveFocus();

        await fireClick(filterDropdown.getByRole("button", { name: /Sink database name/ }));

        expect(filterDropdown.getByPlaceholderText("e.g. name")).toHaveFocus();

        await user.keyboard("berlin{Enter}");

        expect(filterToggle).toHaveAttribute("aria-expanded", "false");
        expect(filterToggle).toHaveFocus();
        expect(container.getByTitle(/Change "Sink database name" filter/)).toHaveTextContent(
            /Sink database name:\s*berlin/
        );

        const chipToggle = container.getByTitle(/Change "Replication state" filter/);
        const chipDropdown = within(chipToggle.parentElement.parentElement);
        chipToggle.focus();
        await user.keyboard("{Enter}");

        expect(chipDropdown.getByLabelText("Up to date")).toHaveFocus();
    });

    it("picks an option and closes the filter menu on Enter", async () => {
        const Story = composeStory(stories.ManySinks, stories.default);

        const { screen, fireClick, user } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        await fireClick(await container.findByTitle(/Click for details/));

        const filterToggle = container.getByTitle("Filter sinks");
        const filterDropdown = within(filterToggle.parentElement);
        await fireClick(filterToggle);
        await fireClick(await filterDropdown.findByRole("button", { name: /Replication state/ }));

        expect(filterDropdown.getByLabelText("Up to date")).toHaveFocus();

        await user.keyboard("{Enter}");

        expect(filterToggle).toHaveFocus();
        expect(filterDropdown.queryByLabelText("Up to date")).not.toBeInTheDocument();
        expect(container.getByTitle(/Change "Replication state" filter/)).toHaveTextContent(
            /Replication state:\s*Up to date/
        );

        await user.keyboard("{Enter}{ArrowDown}{ArrowDown}{ArrowDown}{ArrowDown}");

        expect(filterDropdown.getByRole("button", { name: /Has errors/ })).toHaveFocus();

        await user.keyboard("{Enter}");

        expect(filterDropdown.getByRole("button", { name: "Yes" })).toHaveFocus();

        await user.keyboard("{ArrowDown}{Enter}");

        expect(filterToggle).toHaveFocus();
        expect(filterDropdown.queryByRole("button", { name: "No" })).not.toBeInTheDocument();
        expect(container.getByTitle(/Change "Has errors" filter/)).toHaveTextContent(/Has errors:\s*No/);
    });

    it("moves between filter options with arrow keys", async () => {
        const Story = composeStory(stories.ManySinks, stories.default);

        const { screen, fireClick, user } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        await fireClick(await container.findByTitle(/Click for details/));

        const filterToggle = container.getByTitle("Filter sinks");
        const filterDropdown = within(filterToggle.parentElement);
        await fireClick(filterToggle);
        await fireClick(await filterDropdown.findByRole("button", { name: /Replication state/ }));

        expect(filterDropdown.getByLabelText("Up to date")).toHaveFocus();

        await user.keyboard("{ArrowDown}{ArrowDown}");
        expect(filterDropdown.getByLabelText("Disabled")).toHaveFocus();

        await user.keyboard("{ArrowUp}");
        expect(filterDropdown.getByLabelText("Running")).toHaveFocus();

        await user.keyboard("{ArrowDown}{ArrowDown}{ArrowDown}");
        expect(filterDropdown.getByLabelText("Unknown")).toHaveFocus();

        await user.keyboard(" ");

        expect(filterDropdown.getByLabelText("Unknown")).toBeChecked();
        expect(container.getByTitle(/Change "Replication state" filter/)).toHaveTextContent(
            /Replication state:\s*Unknown/
        );
    });

    it("picks one option for the has errors filter", async () => {
        const Story = composeStory(stories.ManySinks, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);
        const container = within(await screen.findByTestId(containerTestId));

        await fireClick(await container.findByTitle(/Click for details/));

        const filterToggle = container.getByTitle("Filter sinks");
        const filterDropdown = within(filterToggle.parentElement);
        await fireClick(filterToggle);
        await fireClick(await filterDropdown.findByRole("button", { name: /Has errors/ }));

        const clearOption = filterDropdown.getByRole("button", { name: "Clear" });
        expect(clearOption).toHaveAttribute("aria-disabled", "true");

        await fireClick(filterDropdown.getByRole("button", { name: "Yes" }));

        expect(filterDropdown.getByRole("button", { name: "Yes" })).toHaveAttribute("aria-pressed", "true");
        expect(filterDropdown.getByRole("button", { name: "No" })).toHaveAttribute("aria-pressed", "false");
        expect(container.getByTitle(/Change "Has errors" filter/)).toHaveTextContent(/Has errors:\s*Yes/);
        expect(container.getAllByTitle(/^Expand distribution details for /)).toHaveLength(3);

        await fireClick(filterDropdown.getByRole("button", { name: "Yes" }));

        expect(filterDropdown.getByRole("button", { name: "Yes" })).toHaveAttribute("aria-pressed", "true");

        await fireClick(filterDropdown.getByRole("button", { name: "Clear" }));

        expect(container.queryByTitle(/Change "Has errors" filter/)).not.toBeInTheDocument();
        expect(container.getAllByTitle(/^Expand distribution details for /)).toHaveLength(15);
    });
});
