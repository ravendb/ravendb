import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import { act, rtlRender, waitFor } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { mockJQueryError } from "test/mocks/utils";
import notificationCenter from "common/notifications/notificationCenter";
import * as stories from "components/pages/database/tasks/importData/migrateRavenDb/stories/MigrateRavenDbDatabase.stories";

type BuildInfoWithResourceNames = Raven.Server.Smuggler.Migration.BuildInfoWithResourceNames;
type Rendered = ReturnType<typeof rtlRender>;

const { Default, Sharded, NoOperator, LegacyServer, ServerError } = composeStories(stories);

const SERVER_URL = "http://raven-old-1:8080";
const OTHER_SERVER_URL = "https://raven-old-2:8080";

const LEGACY_BUILD_INFO: BuildInfoWithResourceNames = {
    ...TasksStubs.remoteServerBuildInfo(),
    MajorVersion: "V35",
    BuildVersion: 35286,
    ProductVersion: "3.5",
    FullVersion: "",
};

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
    let openDetails: jest.SpyInstance;

    beforeEach(() => {
        jest.useFakeTimers();
        openDetails = jest
            .spyOn(notificationCenter.instance, "openDetailsForOperationById")
            .mockImplementation(() => {});
        tasksMocks().getRemoteServerBuildInfo.mockClear();
        tasksMocks().migrateRavenDbDatabase.mockClear();
    });

    afterEach(() => {
        openDetails.mockRestore();
        jest.useRealTimers();
    });

    it("detects the source version and migrates the chosen database", async () => {
        const rendered = rtlRender(<Default />);
        const { screen, fillInput, fireClick } = rendered;

        expect(getSwitch(rendered, "Include Documents")).toBeDisabled();

        await detectVersion(rendered);

        await waitFor(() => expect(screen.getByLabelText("Server version")).toHaveValue("7.1.3"));
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
    });

    it("disables the toggles the source version does not support", async () => {
        const rendered = rtlRender(<Default sourceVersion="V54" />);

        await detectVersion(rendered);

        expect(await rendered.screen.findByDisplayValue("5.4.121")).toBeInTheDocument();
        expect(getSwitch(rendered, "Include Time Series")).toBeEnabled();
        expect(getSwitch(rendered, "Include Time Series Deleted Ranges")).toBeDisabled();
    });

    it("warns when document extensions are migrated without the documents", async () => {
        const rendered = rtlRender(<Default />);
        await detectVersion(rendered);
        expect(await rendered.screen.findByDisplayValue("7.1.3")).toBeInTheDocument();

        await rendered.fireClick(getSwitch(rendered, "Include Documents"));

        expect(
            rendered.screen.getByText(
                /You are migrating Counters, Time Series, Revisions data without including the Documents/
            )
        ).toBeInTheDocument();
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
        await fillInput(screen.getByLabelText("API Key"), "migration/s3cr3t-k3y");

        expect(getSwitch(rendered, "Include Documents")).toBeEnabled();
        await act(() => jest.advanceTimersByTimeAsync(700));

        const { getRemoteServerBuildInfo } = tasksMocks();
        expect(getRemoteServerBuildInfo).toHaveBeenLastCalledWith(
            expect.objectContaining({ apiKey: "migration/s3cr3t-k3y" })
        );
        expect(getSwitch(rendered, "Include Documents")).toBeEnabled();
    });

    it("keeps the detected version and credential inputs when a credential refetch fails", async () => {
        const rendered = rtlRender(<LegacyServer />);
        const { screen, fillInput, fireClick } = rendered;
        await detectVersion(rendered);
        expect(await screen.findByDisplayValue("3.5 (build: 35286)")).toBeInTheDocument();

        tasksMocks().getRemoteServerBuildInfo.mockRejectedValue(mockJQueryError("Invalid API key"));
        await fireClick(screen.getByRole("tab", { name: "API Key" }));
        await fillInput(screen.getByLabelText("API Key"), "migration");
        await act(() => jest.advanceTimersByTimeAsync(700));

        expect(await screen.findByText("Invalid API key")).toBeInTheDocument();
        expect(screen.getByDisplayValue("3.5 (build: 35286)")).toBeInTheDocument();
        expect(screen.getByLabelText("API Key")).toHaveValue("migration");
        expect(screen.getByRole("button", { name: /Migrate database/ })).toBeDisabled();
    });

    it("blocks Migrate while the legacy server reports unauthorized access", async () => {
        const rendered = rtlRender(<LegacyServer />);
        tasksMocks().getRemoteServerBuildInfo.mockResolvedValue({ ...LEGACY_BUILD_INFO, Authorized: false });

        await detectVersion(rendered);
        await rendered.fillInput(rendered.screen.getByRole("combobox", { name: "Database name" }), "Northwind");

        expect(await rendered.screen.findByText(/Unauthorized access to the server/)).toBeInTheDocument();
        expect(rendered.screen.getByRole("button", { name: /Migrate database/ })).toBeDisabled();
    });

    it("drops legacy credentials when the url switches to a 4.x+ server", async () => {
        const rendered = rtlRender(<Default />);
        const { screen, fillInput, fireClick } = rendered;
        tasksMocks().getRemoteServerBuildInfo.mockImplementation(async (args) =>
            args.serverUrl === SERVER_URL ? LEGACY_BUILD_INFO : TasksStubs.remoteServerBuildInfo()
        );

        await detectVersion(rendered, SERVER_URL);
        expect(await screen.findByDisplayValue("3.5 (build: 35286)")).toBeInTheDocument();
        await fireClick(screen.getByRole("tab", { name: "Windows Credentials" }));
        await fillInput(screen.getByLabelText("User name"), "john.doe");

        await detectVersion(rendered, OTHER_SERVER_URL);
        expect(await screen.findByDisplayValue("7.1.3")).toBeInTheDocument();
        await fillInput(screen.getByRole("combobox", { name: "Database name" }), "Northwind");
        const migrateButton = screen.getByRole("button", { name: /Migrate database/ });
        await waitFor(() => expect(migrateButton).toBeEnabled());
        await fireClick(migrateButton);

        const { migrateRavenDbDatabase } = tasksMocks();
        await waitFor(() => expect(migrateRavenDbDatabase).toHaveBeenCalledTimes(1));
        const [, dto] = migrateRavenDbDatabase.mock.calls[0];
        expect(dto).toMatchObject({
            ServerUrl: OTHER_SERVER_URL,
            UserName: null,
            Password: null,
            Domain: null,
            ApiKey: null,
        });
    });

    it("shows the error returned for the source server", async () => {
        const rendered = rtlRender(<ServerError />);

        await detectVersion(rendered);

        expect(await rendered.screen.findByText(/Could not connect to the source server/)).toBeInTheDocument();
        expect(getSwitch(rendered, "Include Documents")).toBeDisabled();
    });

    it("skips version detection and explains the missing access below Operator", async () => {
        const rendered = rtlRender(<NoOperator />);
        const { screen, user } = rendered;

        await detectVersion(rendered);

        expect(
            screen.getByText(
                "Migrating requires Operator security clearance (needed to detect the source server version)."
            )
        ).toBeInTheDocument();
        expect(tasksMocks().getRemoteServerBuildInfo).not.toHaveBeenCalled();

        const migrateButton = screen.getByRole("button", { name: /Migrate database/ });
        expect(migrateButton).toBeDisabled();
        jest.useRealTimers();
        await user.hover(migrateButton);
        expect(await screen.findByText(/Database Admin access required/)).toBeInTheDocument();
    });

    it("sends no credentials to a new url until it is detected as a legacy source", async () => {
        const rendered = rtlRender(<LegacyServer />);
        const { screen, fillInput, fireClick } = rendered;
        await detectVersion(rendered);
        expect(await screen.findByDisplayValue("3.5 (build: 35286)")).toBeInTheDocument();
        await fireClick(screen.getByRole("tab", { name: "API Key" }));
        await fillInput(screen.getByLabelText("API Key"), "migration/s3cr3t-k3y");
        await act(() => jest.advanceTimersByTimeAsync(700));

        await detectVersion(rendered, OTHER_SERVER_URL);

        expect(tasksMocks().getRemoteServerBuildInfo).toHaveBeenLastCalledWith(
            expect.objectContaining({ serverUrl: OTHER_SERVER_URL, apiKey: "", userName: "", password: "" })
        );
    });

    it("migrates RavenFS from a legacy source with only the server section", async () => {
        const rendered = rtlRender(<LegacyServer />);
        const { screen, fillInput, fireClick } = rendered;
        await detectVersion(rendered);
        expect(await screen.findByDisplayValue("3.5 (build: 35286)")).toBeInTheDocument();

        await fireClick(screen.getByLabelText("RavenFS"));
        await fillInput(screen.getByRole("combobox", { name: "File system name" }), "Files");

        expect(screen.queryByText("Data to import")).not.toBeInTheDocument();
        const migrateButton = screen.getByRole("button", { name: /Migrate database/ });
        await waitFor(() => expect(migrateButton).toBeEnabled());
        await fireClick(migrateButton);

        const { migrateRavenDbDatabase } = tasksMocks();
        await waitFor(() => expect(migrateRavenDbDatabase).toHaveBeenCalledTimes(1));
        const [, dto] = migrateRavenDbDatabase.mock.calls[0];
        expect(dto.MigrationSettings).toMatchObject({
            DatabaseName: "Files",
            ImportRavenFs: true,
            OperateOnTypes: "None",
        });
    });

    it("shows the feature as not available on a sharded database", () => {
        const { screen } = rtlRender(<Sharded />);

        expect(screen.getByText(/is not available for/)).toBeInTheDocument();
        expect(screen.queryByRole("button", { name: /Migrate database/ })).not.toBeInTheDocument();
    });
});
