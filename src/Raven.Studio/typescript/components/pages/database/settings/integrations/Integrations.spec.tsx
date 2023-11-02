import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { BelowDatabaseAdmin, NoLimits } from "./Integrations.stories";

describe("Integrations", () => {
    it("has full access", async () => {
        const { screen } = rtlRender(<NoLimits />);

        expect(await screen.findByText("Add new")).toBeInTheDocument();
    });

    it("has limited access'", async () => {
        const { screen } = rtlRender(<BelowDatabaseAdmin />);

        const addScriptButton = screen.queryByName("addNewCredentialsButton");
        expect(addScriptButton).toBeNull();
    });
});
