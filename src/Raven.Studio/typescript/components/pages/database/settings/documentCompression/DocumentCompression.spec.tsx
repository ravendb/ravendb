import React from "react";
import { rtlRender, RtlScreen, within } from "test/rtlTestUtils";
import * as stories from "./DocumentCompression.stories";
import { composeStories } from "@storybook/react";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultDocumentCompression } = composeStories(stories);

const licenseBadgeText = "Enterprise";

describe("DocumentCompression", () => {
    async function waitForLoad(screen: RtlScreen) {
        await screen.findByText(DatabasesStubs.documentsCompressionConfiguration().Collections[0]);
    }

    it("can render when feature is in license and database access is admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentCompression databaseAccess="DatabaseAdmin" hasDocumentsCompression />
        );

        await waitForLoad(screen);

        expect(screen.queryByClassName("badge")).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Save/ })).toBeInTheDocument();
    });

    it("can render when feature is in license and database access is below admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentCompression databaseAccess="DatabaseRead" hasDocumentsCompression />
        );

        await waitForLoad(screen);

        expect(screen.queryByClassName("badge")).not.toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Save/ })).not.toBeInTheDocument();
    });

    it("can render when feature is not in license and database access is admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentCompression databaseAccess="DatabaseAdmin" hasDocumentsCompression={false} />
        );

        await waitForLoad(screen);

        expect(within(screen.queryByClassName("badge")).queryByText(licenseBadgeText)).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Save/ })).toBeInTheDocument();
    });

    it("can render when feature is not in license and database access is below admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentCompression databaseAccess="DatabaseRead" hasDocumentsCompression={false} />
        );

        await waitForLoad(screen);

        expect(within(screen.queryByClassName("badge")).queryByText(licenseBadgeText)).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Save/ })).not.toBeInTheDocument();
    });
});
