import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { act, rtlRender, waitFor } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import notificationCenter from "common/notifications/notificationCenter";
import * as stories from "./stories/MigrateRavenDbDatabase.stories";

type BuildInfoWithResourceNames = Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames;
type Rendered = ReturnType<typeof rtlRender>;

const { Default, Sharded, NoDatabaseAdmin, NoOperator, LegacyServer, ServerError } = composeStories(stories);

const SERVER_URL = "http://raven-old-1:8080";
const OTHER_SERVER_URL = "https://raven-old-2:8080";

const tasksMocks = () => ({
    getRemoteServerBuildInfo: jest.mocked(mockServices.tasksService.mock.getRemoteServerBuildInfo),
    migrateRavenDbDatabase: jest.mocked(mockServices.tasksService.mock.migrateRavenDbDatabase),
});

const getSwitch = ({ screen }: Rendered, label: string) => screen.getByLabelText(label);

async function typeServerUrl({ screen, fillInput }: Rendered, url: string) {
    await fillInput(screen.getByRole("combobox", { name: "Server URL" }), url);
}

async function detectVersion(rendered: Rendered, url = SERVER_URL) {
    await typeServerUrl(rendered, url);
    await act(() => jest.advanceTimersByTimeAsync(700));
}

describe("MigrateRavenDbDatabase", () => {
    beforeEach(() => {
        jest.useFakeTimers();
        tasksMocks().getRemoteServerBuildInfo.mockClear();
        tasksMocks().migrateRavenDbDatabase.mockClear();
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it("detects the source version and migrates the chosen database", async () => {
        const openDetails = jest
            .spyOn(notificationCenter.instance, "openDetailsForOperationById")
            .mockImplementation(() => {});
        const rendered = rtlRender(<Default />);
        const { screen, fillInput, fireClick } = rendered;

        expect(getSwitch(rendered, "Include Documents")).toBeDisabled();

        await detectVersion(rendered);

        expect(await screen.findByDisplayValue("7.1.3")).toBeInTheDocument();
        expect(getSwitch(rendered, "Include Documents")).toBeEnabled();

        await fillInput(screen.getByRole("combobox", { name: "Database name" }), "Northwind");
        const migrateButton = screen.getByRole("button", { name: /Migrate database/ });
        await waitFor(() => expect(migrateButton).toBeEnabled());
        await fireClick(migrateButton);

        const { migrateRavenDbDatabase } = tasksMocks();
        await waitFor(() => expect(migrateRavenDbDatabase).toHaveBeenCalledTimes(1));
        const [databaseName, dto] = migrateRavenDbDatabase.mock.calls[0];
        expect(dto.ServerUrl).toBe(SERVER_URL);
        expect(dto.MigrationSettings.DatabaseName).toBe("Northwind");
        expect(dto.BuildMajorVersion).toBe("V7");
        await waitFor(() => expect(openDetails).toHaveBeenCalledWith(databaseName, 1234));

        openDetails.mockRestore();
    });

    it("disables the toggles the source version does not support", async () => {
        const rendered = rtlRender(<Default sourceVersion="V54" />);

        await detectVersion(rendered);

        expect(await rendered.screen.findByDisplayValue("5.4.121")).toBeInTheDocument();
        expect(getSwitch(rendered, "Include Time Series")).toBeEnabled();
        expect(getSwitch(rendered, "Include Time Series Deleted Ranges")).toBeDisabled();
    });

    it("locks the sections again as soon as the server url changes", async () => {
        const rendered = rtlRender(<Default />);
        await detectVersion(rendered);
        expect(await rendered.screen.findByDisplayValue("7.1.3")).toBeInTheDocument();

        await typeServerUrl(rendered, OTHER_SERVER_URL);

        expect(rendered.screen.queryByDisplayValue("7.1.3")).not.toBeInTheDocument();
        expect(getSwitch(rendered, "Include Documents")).toBeDisabled();
    });

    it("shows the latest server when an older response arrives last", async () => {
        const rendered = rtlRender(<Default />);
        const resolvers = new Map<string, (info: BuildInfoWithResourceNames) => void>();
        tasksMocks().getRemoteServerBuildInfo.mockImplementation(
            (args) => new Promise((resolve) => resolvers.set(args.serverUrl, resolve))
        );

        await detectVersion(rendered, SERVER_URL);
        await detectVersion(rendered, OTHER_SERVER_URL);

        await act(async () => resolvers.get(OTHER_SERVER_URL)(TasksStubs.remoteServerBuildInfo()));
        await act(async () =>
            resolvers.get(SERVER_URL)({
                ...TasksStubs.remoteServerBuildInfo(),
                MajorVersion: "V5",
                FullVersion: "5.4.121",
            })
        );

        expect(rendered.screen.getByDisplayValue("7.1.3")).toBeInTheDocument();
        expect(rendered.screen.queryByDisplayValue("5.4.121")).not.toBeInTheDocument();
    });

    it("shows the legacy options and refetches silently when credentials change", async () => {
        const rendered = rtlRender(<LegacyServer />);
        const { screen, fillInput, fireClick } = rendered;

        await detectVersion(rendered);

        expect(await screen.findByDisplayValue("3.5 (build: 35286)")).toBeInTheDocument();
        expect(getSwitch(rendered, "Include Legacy Attachments")).toBeEnabled();
        expect(getSwitch(rendered, "Include Attachments")).toBeDisabled();

        await fireClick(screen.getByRole("tab", { name: "API Key" }));
        await fillInput(screen.getByLabelText("API Key"), "secret");

        expect(getSwitch(rendered, "Include Documents")).toBeEnabled();
        await act(() => jest.advanceTimersByTimeAsync(700));

        const { getRemoteServerBuildInfo } = tasksMocks();
        expect(getRemoteServerBuildInfo).toHaveBeenLastCalledWith(expect.objectContaining({ apiKey: "secret" }));
        expect(getSwitch(rendered, "Include Documents")).toBeEnabled();
    });

    it("shows the error returned for the source server", async () => {
        const rendered = rtlRender(<ServerError />);

        await detectVersion(rendered);

        expect(await rendered.screen.findByText(/Could not connect to the source server/)).toBeInTheDocument();
        expect(getSwitch(rendered, "Include Documents")).toBeDisabled();
    });

    it("keeps Migrate disabled without database admin access", async () => {
        const rendered = rtlRender(<NoDatabaseAdmin />);

        await detectVersion(rendered);

        expect(rendered.screen.getByRole("button", { name: /Migrate database/ })).toBeDisabled();
    });

    it("skips version detection below Operator security clearance", async () => {
        const rendered = rtlRender(<NoOperator />);

        await detectVersion(rendered);

        expect(
            rendered.screen.getByText("Detecting the source server version requires Operator security clearance.")
        ).toBeInTheDocument();
        expect(tasksMocks().getRemoteServerBuildInfo).not.toHaveBeenCalled();
    });

    it("shows the feature as not available on a sharded database", () => {
        const { screen } = rtlRender(<Sharded />);

        expect(screen.getByText(/is not available for/)).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Migrate database/ })).not.toBeInTheDocument();
    });
});
