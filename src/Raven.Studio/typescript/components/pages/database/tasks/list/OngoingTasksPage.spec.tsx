import { rtlRender } from "../../../../../test/rtlTestUtils";
import React from "react";
import { OngoingTasksPage } from "./OngoingTasksPage";

import * as stories from "./OngoingTasksPage.stories";
import { composeStories, composeStory } from "@storybook/testing-react";
import { boundCopy } from "../../../../utils/common";

const { EmptyView, FullView } = composeStories(stories);

const selectors = {
    emptyScriptText: /Following scripts don't match any documents/i,
} as const;

describe("OngoingTasksPage", function () {
    it("can render empty view", async () => {
        const { screen } = rtlRender(<EmptyView />);

        expect(await screen.findByText(/No tasks have been created for this Database Group/)).toBeInTheDocument();
    });

    it("can render full view", async () => {
        const { screen } = rtlRender(<FullView />);

        expect(await screen.findByText(/RavenDB ETL/)).toBeInTheDocument();
        //TODO: other assertions
    });

    describe("RavenETL", function () {
        it("can render disabled and not completed", async () => {
            const View = boundCopy(stories.RavenEtlTemplate, {
                disabled: true,
                completed: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/RavenDB ETL/)).toBeInTheDocument();
            expect(await screen.findByText(/Disabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Enabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Topology Discovery URLs/)).toBeInTheDocument();

            //wait for progress
            await screen.findAllByText(/Disabled/i);
        });

        it("can render completed", async () => {
            const View = boundCopy(stories.RavenEtlTemplate, {
                completed: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText(/Up to date/i);
        });

        it("can render enabled and not completed", async () => {
            const View = boundCopy(stories.RavenEtlTemplate, {
                completed: false,
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText("Running");
        });

        it("can notify about empty script", async () => {
            const View = boundCopy(stories.RavenEtlTemplate, {
                completed: true,
                emptyScript: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText(/Up to date/i);

            expect(await screen.findByText(selectors.emptyScriptText)).toBeInTheDocument();
        });
    });

    describe("SQL", function () {
        it("can render disabled and not completed", async () => {
            const View = boundCopy(stories.SqlTemplate, {
                disabled: true,
                completed: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/SQL ETL/)).toBeInTheDocument();
            expect(await screen.findByText(/Disabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Enabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            const target = await screen.findByTitle("Destination <database>@<server>");
            expect(target).toBeInTheDocument();

            //wait for progress
            await screen.findAllByText(/Disabled/i);
        });

        it("can render completed", async () => {
            const View = boundCopy(stories.SqlTemplate, {
                completed: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText(/Up to date/i);
        });

        it("can render enabled and not completed", async () => {
            const View = boundCopy(stories.SqlTemplate, {
                completed: false,
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText("Running");
        });

        it("can notify about empty script", async () => {
            const View = boundCopy(stories.SqlTemplate, {
                completed: true,
                emptyScript: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText(/Up to date/i);

            expect(await screen.findByText(selectors.emptyScriptText)).toBeInTheDocument();
        });
    });

    describe("OLAP", function () {
        it("can render disabled and not completed", async () => {
            const View = boundCopy(stories.OlapTemplate, {
                disabled: true,
                completed: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/OLAP ETL/)).toBeInTheDocument();
            expect(await screen.findByText(/Disabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Enabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Destination/)).toBeInTheDocument();

            expect(await screen.findByText(/Connection String/)).toBeInTheDocument();

            //wait for progress
            await screen.findAllByText(/Disabled/i);
        });

        it("can render completed", async () => {
            const View = boundCopy(stories.OlapTemplate, {
                completed: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText(/Up to date/i);
        });

        it("can render enabled and not completed", async () => {
            const View = boundCopy(stories.OlapTemplate, {
                completed: false,
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText("Running");
        });

        it("can notify about empty script", async () => {
            const View = boundCopy(stories.OlapTemplate, {
                completed: true,
                emptyScript: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText(/Up to date/i);

            expect(await screen.findByText(selectors.emptyScriptText)).toBeInTheDocument();
        });
    });

    describe("ElasticSearch", function () {
        it("can render disabled and not completed", async () => {
            const View = boundCopy(stories.ElasticSearchTemplate, {
                disabled: true,
                completed: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/Elasticsearch ETL/)).toBeInTheDocument();
            expect(await screen.findByText(/Disabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Enabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText("http://elastic1:8081")).toBeInTheDocument();

            expect(await screen.findByText(/Connection String/)).toBeInTheDocument();

            //wait for progress
            await screen.findAllByText(/Disabled/i);
        });

        it("can render completed", async () => {
            const View = boundCopy(stories.ElasticSearchTemplate, {
                completed: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText(/Up to date/i);
        });

        it("can render enabled and not completed", async () => {
            const View = boundCopy(stories.ElasticSearchTemplate, {
                completed: false,
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText("Running");
        });

        it("can notify about empty script", async () => {
            const View = boundCopy(stories.ElasticSearchTemplate, {
                completed: true,
                emptyScript: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);

            //wait for progress
            await screen.findAllByText(/Up to date/i);

            expect(await screen.findByText(selectors.emptyScriptText)).toBeInTheDocument();
        });
    });
});
