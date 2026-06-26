import React from "react";
import { rtlRender, RtlScreen } from "test/rtlTestUtils";
import * as stories from "./DocumentCompression.stories";
import { composeStories } from "@storybook/react-webpack5";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultDocumentCompression } = composeStories(stories);

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

        expect(screen.queryByTestId("license-restricted-badge")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Save/ })).toBeInTheDocument();
    });

    it("can render when feature is not in license and database access is below admin", async () => {
        const { screen } = rtlRender(
            <DefaultDocumentCompression databaseAccess="DatabaseRead" hasDocumentsCompression={false} />
        );

        await waitForLoad(screen);

        expect(screen.queryByTestId("license-restricted-badge")).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Save/ })).not.toBeInTheDocument();
    });
});
