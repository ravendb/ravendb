import { composeStories } from "@storybook/react-webpack5";
import { rtlRender, waitFor } from "test/rtlTestUtils";
import * as stories from "./TombstonesState.stories";
import React from "react";
import { mockServices } from "test/mocks/services/MockServices";
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

const { Tombstones } = composeStories(stories);

describe("TombstonesState", () => {
    it("can render", async () => {
        const { screen } = rtlRender(<Tombstones />);

        expect(await screen.findByRole("heading", { name: /Per Collection/ })).toBeInTheDocument();
        expect(await screen.findByRole("heading", { name: /Per Task/ })).toBeInTheDocument();
    });

    it("shows ~ prefix for estimated tombstone counts", async () => {
        const { screen } = rtlRender(<Tombstones />);

        const format = (n: number) => n.toLocaleString();

        expect(await screen.findByText(`~${format(1500)}`)).toBeInTheDocument();
        expect(
            await screen.findByText(`Documents: ~${format(1200)}, TimeSeries: ~200, Counters: 0`)
        ).toBeInTheDocument();
    });

    it("can calculate the exact tombstone counts", async () => {
        const getTombstonesState = jest.mocked(mockServices.databasesService.mock.getTombstonesState);

        try {
            const { screen, fireClick } = rtlRender(<Tombstones />);

            expect(await screen.findByText(`~${(1500).toLocaleString()}`)).toBeInTheDocument();

            const exactState = DatabasesStubs.tombstonesState();
            exactState.PerSubscriptionInfoExtended.forEach((x) => (x.Estimated = false));
            getTombstonesState.mockResolvedValue(exactState);

            await fireClick(screen.getByRole("button", { name: /Calculate exact/ }));

            expect(getTombstonesState).toHaveBeenLastCalledWith(expect.any(String), undefined, true);
            await waitFor(() => expect(screen.queryByText(/~/)).not.toBeInTheDocument());
            expect(screen.queryByRole("button", { name: /Calculate exact/ })).not.toBeInTheDocument();
        } finally {
            getTombstonesState.mockReset();
        }
    });
});
