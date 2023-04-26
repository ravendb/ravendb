import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { ClientConfiguration } from "./ClientGlobalConfiguration.stories";

describe("ClientGlobalConfiguration", function () {
    it("can render", async () => {
        const { screen } = rtlRender(<ClientConfiguration />);

        expect(await screen.findByText(/Save/)).toBeInTheDocument();
    });
});
