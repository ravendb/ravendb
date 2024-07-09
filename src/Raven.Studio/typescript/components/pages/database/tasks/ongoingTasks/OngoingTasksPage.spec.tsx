import { rtlRender } from "test/rtlTestUtils";
import React from "react";

import * as stories from "./OngoingTasksPage.stories";
import { composeStories, composeStory } from "@storybook/react";
import { boundCopy } from "components/utils/common";

const { EmptyView, FullView } = composeStories(stories);

const selectors = {
    emptyScriptText: /Following scripts don't match any documents/i,
    deleteTaskTitle: /Delete task/,
    editTaskTitle: /Edit task/,
} as const;

describe("OngoingTasksPage", function () {
    it("can render empty view", async () => {
        const { screen } = rtlRender(<EmptyView />);

        expect(await screen.findByText(/No tasks have been created for this Database Group/)).toBeInTheDocument();
    });

    it("can render full view", async () => {
        const { screen } = rtlRender(<FullView />);

        expect(await screen.findByText(/RavenDB ETL/)).toBeInTheDocument();
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

    describe("Kafka ETL", function () {
        it("can render disabled and not completed", async () => {
            const View = boundCopy(stories.KafkaEtlTemplate, {
                disabled: true,
                completed: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/KAFKA ETL/)).toBeInTheDocument();
            expect(await screen.findByText(/Disabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Enabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Connection String/)).toBeInTheDocument();

            //wait for progress
            await screen.findAllByText(/Disabled/i);
        });

        it("can render completed", async () => {
            const View = boundCopy(stories.KafkaEtlTemplate, {
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
            const View = boundCopy(stories.KafkaEtlTemplate, {
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
            const View = boundCopy(stories.KafkaEtlTemplate, {
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

    describe("RabbitMQ ETL", function () {
        it("can render disabled and not completed", async () => {
            const View = boundCopy(stories.RabbitEtlTemplate, {
                disabled: true,
                completed: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/RabbitMQ ETL/i)).toBeInTheDocument();
            expect(await screen.findByText(/Disabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Enabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Connection String/)).toBeInTheDocument();

            //wait for progress
            await screen.findAllByText(/Disabled/i);
        });

        it("can render completed", async () => {
            const View = boundCopy(stories.RabbitEtlTemplate, {
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
            const View = boundCopy(stories.RabbitEtlTemplate, {
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
            const View = boundCopy(stories.RabbitEtlTemplate, {
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

    describe("Kafka Sink", function () {
        it("can render enabled", async () => {
            const View = boundCopy(stories.KafkaSinkTemplate, {
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/KAFKA SINK/)).toBeInTheDocument();
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Connection String/)).toBeInTheDocument();
        });

        it("can render disabled", async () => {
            const View = boundCopy(stories.KafkaSinkTemplate, {
                disabled: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);

            expect(await screen.findByText(/Disabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Enabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);
        });
    });

    describe("RabbitMq Sink", function () {
        it("can render enabled", async () => {
            const View = boundCopy(stories.RabbitSinkTemplate, {
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/RABBITMQ SINK/)).toBeInTheDocument();
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Connection String/)).toBeInTheDocument();
        });

        it("can render disabled", async () => {
            const View = boundCopy(stories.RabbitSinkTemplate, {
                disabled: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);

            expect(await screen.findByText(/Disabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Enabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);
            await fireClick(detailsBtn);
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

    describe("Replication Sink", function () {
        it("can render enabled", async () => {
            const View = boundCopy(stories.ReplicationSinkTemplate, {
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByRole("heading", { name: /Replication Sink/ })).toBeInTheDocument();
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Hub Database/)).toBeInTheDocument();
            expect(await screen.findByText(/Connection String/)).toBeInTheDocument();
            expect(await screen.findByText(/Actual Hub URL/)).toBeInTheDocument();
            expect(await screen.findByText(/Hub Name/)).toBeInTheDocument();
        });
    });

    describe("Replication Hub", function () {
        it("can render hub w/o connections", async () => {
            const View = boundCopy(stories.ReplicationHubTemplate, {
                disabled: false,
                withOutConnections: true,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByRole("heading", { name: /Replication Hub/ })).toBeInTheDocument();
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/No sinks connected/)).toBeInTheDocument();
        });

        it("can render hub w/ connections", async () => {
            const View = boundCopy(stories.ReplicationHubTemplate, {
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByRole("heading", { name: /Replication Hub/ })).toBeInTheDocument();
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Task Name/)).toBeInTheDocument();
            expect(await screen.findByText(/Sink Database/)).toBeInTheDocument();
            expect(await screen.findByText(/target-hub-db/)).toBeInTheDocument();
            expect(await screen.findByText(/Actual Sink URL/)).toBeInTheDocument();
        });
    });

    describe("Periodic Backup", function () {
        it("can render enabled", async () => {
            const View = boundCopy(stories.PeriodicBackupTemplate, {
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByText(/Periodic Backup/)).toBeInTheDocument();
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Destinations/)).toBeInTheDocument();
            expect(await screen.findByText(/Last Full Backup/)).toBeInTheDocument();
            expect(await screen.findByText(/Last Incremental Backup/)).toBeInTheDocument();
            expect(await screen.findByText(/Next Estimated Backup/)).toBeInTheDocument();
            expect(await screen.findByText(/Retention Policy/)).toBeInTheDocument();
        });
    });

    describe("External Replication", function () {
        it("can render enabled", async () => {
            const View = boundCopy(stories.ExternalReplicationTemplate, {
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByRole("heading", { name: /External Replication/ })).toBeInTheDocument();
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Connection String/)).toBeInTheDocument();
            expect(await screen.findByText(/Destination Database/)).toBeInTheDocument();
            expect(await screen.findByText(/Actual Destination URL/)).toBeInTheDocument();
            expect(await screen.findByText(/Topology Discovery URLs/)).toBeInTheDocument();

            // edit, delete button should be present for non-server wide
            expect(screen.queryByTitle(selectors.deleteTaskTitle)).toBeInTheDocument();
            expect(screen.queryByTitle(selectors.editTaskTitle)).toBeInTheDocument();
        });

        it("can render server wide", async () => {
            const Story = composeStory(stories.ExternalReplicationServerWide, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            // edit, delete button not present for server wide
            expect(screen.queryByTitle(selectors.deleteTaskTitle)).not.toBeInTheDocument();
            expect(screen.queryByTitle(selectors.editTaskTitle)).not.toBeInTheDocument();
        });
    });

    describe("Subscription", function () {
        it("can render enabled", async () => {
            const View = boundCopy(stories.SubscriptionTemplate, {
                disabled: false,
            });

            const Story = composeStory(View, stories.default);

            const { screen, fireClick } = rtlRender(<Story />);
            expect(await screen.findByRole("heading", { name: /Subscription/ })).toBeInTheDocument();
            expect(await screen.findByText(/Enabled/)).toBeInTheDocument();
            expect(screen.queryByText(/Disabled/)).not.toBeInTheDocument();

            const detailsBtn = await screen.findByTitle(/Click for details/);

            await fireClick(detailsBtn);

            expect(await screen.findByText(/Last Batch Ack Time/)).toBeInTheDocument();
            expect(await screen.findByText(/Last Client Connection Time/)).toBeInTheDocument();
            expect(await screen.findByText(/Change vector for next batch/)).toBeInTheDocument();
        });
    });
});
