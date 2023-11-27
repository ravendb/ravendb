import React from "react";
import { composeStories } from "@storybook/react";
import * as stories from "./ConnectionStrings.stories";
import { rtlRender } from "test/rtlTestUtils";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultConnectionStrings } = composeStories(stories);

const emptyListText = "No connection strings";

describe("ConnectionStrings", () => {
    it("can render empty list", async () => {
        const { screen } = rtlRender(<DefaultConnectionStrings isEmpty />);

        expect(await screen.findByText(emptyListText)).toBeInTheDocument();
    });
    it("", async () => {
        const { screen } = rtlRender(<DefaultConnectionStrings />);

        const connectionStrings = DatabasesStubs.connectionStrings();

        // TODO
        expect(await screen.findByText(Object.keys(connectionStrings.RavenConnectionStrings)[0])).toBeInTheDocument();
        expect(await screen.findByText(Object.keys(connectionStrings.SqlConnectionStrings)[0])).toBeInTheDocument();
        expect(await screen.findByText(Object.keys(connectionStrings.OlapConnectionStrings)[0])).toBeInTheDocument();
        expect(
            await screen.findByText(Object.keys(connectionStrings.ElasticSearchConnectionStrings)[0])
        ).toBeInTheDocument();
        expect(await screen.findByText(Object.keys(connectionStrings.QueueConnectionStrings)[0])).toBeInTheDocument();
        expect(await screen.findByText(Object.keys(connectionStrings.QueueConnectionStrings)[1])).toBeInTheDocument();
    });
});
