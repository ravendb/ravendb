import { rtlRender } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react";
import * as stories from "./AboutPage.stories";
import React from "react";

const {
    AboutPage,
    ConnectionFailure,
    NoSupportOnPremise,
    NoSupportCloud,
    ProfessionalSupportOnPremise,
    ProfessionalSupportCloud,
    ProductionSupportCloud,
    ProductionSupportOnPremise,
} = composeStories(stories);

describe("AboutPage", function () {
    it("can retest connection to license server", async () => {
        const screen = rtlRender(<ConnectionFailure />);

        expect(await screen.findByText(selectors.licenseServer.failure)).toBeInTheDocument();
        expect(await screen.findByText(selectors.licenseServer.retestButton)).toBeInTheDocument();
    });

    //TODO: tests for license types
    //TODO: tests for support types
});

const selectors = {
    licenseServer: {
        failure: /Unable to reach the RavenDB License Server/,
        retestButton: /Test again/,
    },
};
