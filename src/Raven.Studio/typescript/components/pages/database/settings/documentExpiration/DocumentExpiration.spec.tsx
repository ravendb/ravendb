import React from "react";
import { composeStories } from "@storybook/react";
import { rtlRender } from "test/rtlTestUtils";
import * as stories from "./DocumentExpiration.stories";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { DefaultDocumentExpiration } = composeStories(stories);

describe("DocumentExpiration", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<DefaultDocumentExpiration />);

        expect(await screen.findByText("Enable Document Expiration")).toBeInTheDocument();
    });

    it("can disable and set to null expiration frequency after disabling 'Enable Document Expiration'", async () => {
        const { screen, fireClick } = rtlRender(<DefaultDocumentExpiration />);

        const deleteFrequencyBefore = await screen.findByName("deleteFrequency");
        expect(deleteFrequencyBefore).toBeEnabled();
        expect(deleteFrequencyBefore).toHaveValue(DatabasesStubs.expirationConfiguration().DeleteFrequencyInSec);

        await fireClick(screen.getByRole("checkbox", { name: "Enable Document Expiration" }));

        const deleteFrequencyAfter = screen.getByName("deleteFrequency");
        expect(deleteFrequencyAfter).toBeDisabled();
        expect(deleteFrequencyAfter).toHaveValue(null);
    });
});
