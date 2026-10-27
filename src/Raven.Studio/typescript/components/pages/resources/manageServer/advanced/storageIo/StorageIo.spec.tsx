import React from "react";
import { rtlRender } from "test/rtlTestUtils";
import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./StorageIo.stories";

const { Saturated, Healthy } = composeStories(stories);

describe("StorageIo", function () {
    it("shows a saturated disk, why, and the environment that needs attention first", async () => {
        const { screen } = rtlRender(<Saturated />);

        expect(await screen.findByText("Disk /")).toBeInTheDocument();
        expect(screen.getByText("Saturated")).toBeInTheDocument();
        expect(screen.getByText("Writes paced (drain)")).toBeInTheDocument();
        expect(screen.getByText("93%")).toBeInTheDocument();
        expect(screen.getByText(/Slow writes in the last hour \(2\)/)).toBeInTheDocument();

        // the mandatory sync of the documents environment is called out
        expect(screen.getAllByText("Mandatory").length).toBeGreaterThan(0);

        // the index that left the shared journal
        expect(screen.getByText(/Writing their own journals/)).toBeInTheDocument();
        expect(screen.getByText(/Orders\/Totals \(/)).toBeInTheDocument();

        expect(screen.getByText("Vectored file I/O")).toBeInTheDocument();
        expect(screen.getByText(/I\/O ring could not be set up when the server started/)).toBeInTheDocument();
    });

    it("shows a healthy disk in plain words", async () => {
        const { screen } = rtlRender(<Healthy />);

        expect(await screen.findByText("OK")).toBeInTheDocument();
        expect(screen.getByText("Normal (trickle)")).toBeInTheDocument();
        expect(screen.queryByText(/Slow writes in the last hour/)).not.toBeInTheDocument();
        expect(screen.getByText("I/O ring")).toBeInTheDocument();
        expect(screen.queryByText(/I\/O ring could not be set up/)).not.toBeInTheDocument();
    });
});
