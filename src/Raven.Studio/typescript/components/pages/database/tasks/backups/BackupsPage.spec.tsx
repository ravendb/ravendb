import { rtlRender } from "test/rtlTestUtils";
import React from "react";

import * as stories from "./BackupsPage.stories";
import { composeStories, composeStory } from "@storybook/react";
import { boundCopy } from "components/utils/common";

const { EmptyView, FullView } = composeStories(stories);

describe("BackupsPage", function () {
    it("can render empty view", async () => {
        const { screen } = rtlRender(<EmptyView />);

        expect(await screen.findByText(/Create a Periodic Backup/)).toBeInTheDocument();
        expect(await screen.findByText(/Create a Backup/)).toBeInTheDocument();
        expect(await screen.findByText(/Restore a database from a backup/)).toBeInTheDocument();

        expect(await screen.findByText(/No periodic backup tasks created/)).toBeInTheDocument();
        expect(await screen.findByText(/No manual backup created/)).toBeInTheDocument();
    });

    it("can render full view", async () => {
        const { screen } = rtlRender(<FullView />);

        expect(await screen.findByText(/Create a Periodic Backup/)).toBeInTheDocument();
        expect(await screen.findByText(/Create a Backup/)).toBeInTheDocument();
        expect(await screen.findByText(/Restore a database from a backup/)).toBeInTheDocument();

        expect(await screen.findByText(/Raven Backup/)).toBeInTheDocument();

        expect(screen.queryByText(/No periodic backup tasks created/)).not.toBeInTheDocument();
        expect(screen.queryByText(/No manual backup created/)).not.toBeInTheDocument();
    });

    describe("Periodic Backup", function () {
        it("can render enabled", async () => {
            const View = boundCopy(stories.PeriodicBackupTemplate, {
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Last Full Backup/)).toBeInTheDocument();
            expect(await screen.findByText(/Last Incremental Backup/)).toBeInTheDocument();
            expect(await screen.findByText(/Next Estimated Backup/)).toBeInTheDocument();
            expect(await screen.findByText(/Retention Policy/)).toBeInTheDocument();
        });
    });
});
