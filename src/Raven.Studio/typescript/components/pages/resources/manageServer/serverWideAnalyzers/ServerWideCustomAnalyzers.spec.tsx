import React from "react";
import { commonSelectors, rtlRender } from "test/rtlTestUtils";
import ServerWideCustomAnalyzers from "./ServerWideCustomAnalyzers";
import { mockServices } from "test/mocks/services/MockServices";
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import { todo } from "common/developmentHelper";

todo("Limits", "Damian", "Test community limits");

describe("ServerWideCustomAnalyzers", () => {
    it("can render loading error", async () => {
        const { manageServerService } = mockServices;
        manageServerService.withThrowingGetServerWideCustomAnalyzers();

        const { screen } = rtlRender(<ServerWideCustomAnalyzers />);

        expect(await screen.findByText(commonSelectors.loadingError)).toBeInTheDocument();
    });

    it("can render empty view", async () => {
        const { manageServerService } = mockServices;
        manageServerService.withGetServerWideCustomAnalyzers([]);

        const { screen } = rtlRender(<ServerWideCustomAnalyzers />);

        expect(await screen.findByText(/No server-wide custom analyzers have been defined/)).toBeInTheDocument();
    });

    it("can render analyzers list", async () => {
        const { manageServerService } = mockServices;
        manageServerService.withGetServerWideCustomAnalyzers();

        const { screen } = rtlRender(<ServerWideCustomAnalyzers />);

        const existingName = ManageServerStubs.getServerWideCustomAnalyzers()[0].Name;
        expect(await screen.findByText(existingName)).toBeInTheDocument();
    });
});
