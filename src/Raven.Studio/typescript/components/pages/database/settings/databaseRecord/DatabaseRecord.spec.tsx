import { composeStories } from "@storybook/react";
import * as Stories from "./DatabaseRecord.stories";
import React from "react";
import { rtlRender } from "test/rtlTestUtils";

const { DefaultDatabaseRecord } = composeStories(Stories);

describe("DatabaseRecord", () => {
    it("can render with security clearance operator", async () => {
        const { screen } = rtlRender(<DefaultDatabaseRecord securityClearance="Operator" />);
        expect(await screen.findByText(/Hide empty values/)).toBeInTheDocument();
    });

    it("can render without security clearance below operator", async () => {
        const { screen } = rtlRender(<DefaultDatabaseRecord securityClearance="ValidUser" />);
        expect(await screen.findByText(/You are not authorized to view this page/)).toBeInTheDocument();
    });
});
