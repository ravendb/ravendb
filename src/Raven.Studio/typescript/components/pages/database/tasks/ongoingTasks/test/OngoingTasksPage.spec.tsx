import { commonSelectors, rtlRender, waitFor } from "test/rtlTestUtils";
import React from "react";

import * as stories from "../stories/OngoingTasksPage.stories";

import { composeStories } from "@storybook/react-webpack5";
import { mockServices } from "test/mocks/services/MockServices";

const { FullView, EmptyView, WithUnreachableNode, WithUnreachableOrchestrator, WithFailingNode, WithAllNodesFailing } =
    composeStories(stories);

describe("OngoingTasksPage", function () {
    it("can render full view", async () => {
        const { screen } = rtlRender(<FullView />);

        expect(await screen.findByText(/RavenDB ETL/)).toBeInTheDocument();
    });

    it("shows empty state when no tasks exist", async () => {
        const { screen } = rtlRender(<EmptyView />);

        expect(await screen.findByText(/No tasks have been created/)).toBeInTheDocument();
    });

    it("renders tasks from reachable nodes when one node never responds", async () => {
        const { screen } = rtlRender(<WithUnreachableNode />);

        expect(await screen.findByText(/RavenDB ETL/)).toBeInTheDocument();
    });

    it("loads shard tasks when an orchestrator never responds", async () => {
        const { screen } = rtlRender(<WithUnreachableOrchestrator />);

        expect(await screen.findByText(/RavenDB ETL/)).toBeInTheDocument();
    });

    it("shows the load error when every node fails", async () => {
        const { screen } = rtlRender(<WithAllNodesFailing />);

        expect(await screen.findByText(commonSelectors.loadingError)).toBeInTheDocument();
    });

    it("marks the failing responsible node in the task distribution", async () => {
        const { screen } = rtlRender(<WithFailingNode />);

        const panel = (await screen.findByText(/RavenDB ETL/)).closest<HTMLElement>(".rich-panel-item");
        const nodeCell = panel.querySelector(".distribution-item");

        expect(nodeCell).toHaveTextContent("C");
        expect(nodeCell.querySelector(".icon-warning")).toBeInTheDocument();
    });

    it("starts polling ETL progress once an ETL task is loaded", async () => {
        const getEtlProgress = jest.mocked(mockServices.tasksService.mock.getEtlProgress);
        getEtlProgress.mockClear();

        rtlRender(<FullView />);

        await waitFor(() => expect(getEtlProgress).toHaveBeenCalled(), { timeout: 2000 });
    });
});
