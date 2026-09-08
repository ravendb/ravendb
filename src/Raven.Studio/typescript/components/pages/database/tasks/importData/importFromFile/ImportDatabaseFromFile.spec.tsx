import React from "react";
import { composeStories } from "@storybook/react-webpack5";
import * as stories from "./stories/ImportDatabaseFromFile.stories";
import { act, rtlRender, waitFor } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { defaultTransformScript } from "./useImportFromFileForm";

type ImportOptions = Raven.Client.Documents.Smuggler.DatabaseSmugglerImportOptions;
type DatabaseItemType = Raven.Client.Documents.Smuggler.DatabaseItemType;
type DatabaseRecordItemType = Raven.Client.Documents.Smuggler.DatabaseRecordItemType;
type LicenseType = Raven.Server.Commercial.LicenseType;
type Rendered = ReturnType<typeof rtlRender>;

const { Default } = composeStories(stories);

const selectors = {
    importButton: /Import database/,
    commandButton: /Use import command/,
    fileInput: "file-input",
    noIncludeAlert: /At least one 'include' option must be checked/,
    licenseAlert: /Some data may not be imported/,
    accessAlert: /Some data cannot be imported/,
};

const adminDefaultOperateOnTypes: DatabaseItemType[] = [
    "DatabaseRecord",
    "Documents",
    "Conflicts",
    "Indexes",
    "RevisionDocuments",
    "Identities",
    "CompareExchange",
    "CounterGroups",
    "Attachments",
    "TimeSeries",
    "TimeSeriesDeletedRanges",
    "Subscriptions",
    "Tombstones",
    "CompareExchangeTombstones",
];

const allSettingTokens: DatabaseRecordItemType[] = [
    "Settings",
    "ConflictSolverConfig",
    "Client",
    "Revisions",
    "Refresh",
    "Expiration",
    "DocumentsCompression",
    "SchemaValidation",
    "DataArchival",
    "TimeSeries",
    "Sorters",
    "Analyzers",
    "PostgreSQLIntegration",
];

const allOngoingTaskTokens: DatabaseRecordItemType[] = [
    "PeriodicBackups",
    "ExternalReplications",
    "RavenEtls",
    "SqlEtls",
    "SnowflakeEtls",
    "OlapEtls",
    "ElasticSearchEtls",
    "QueueEtls",
    "HubPullReplications",
    "SinkPullReplications",
    "EmbeddingsGenerations",
    "GenAiEtls",
    "CdcSinks",
    "AiAgents",
    "RemoteAttachments",
];

const allConnectionStringTokens: DatabaseRecordItemType[] = [
    "RavenConnectionStrings",
    "SqlConnectionStrings",
    "SnowflakeConnectionStrings",
    "OlapConnectionStrings",
    "ElasticSearchConnectionStrings",
    "QueueConnectionStrings",
    "AiConnectionStrings",
];

const without = <T,>(list: T[], ...excluded: T[]) => list.filter((x) => !excluded.includes(x));

const operateOnTypesOf = (dto: ImportOptions) => dto.OperateOnTypes.split(",");
const recordTypesOf = (dto: ImportOptions) => dto.OperateOnDatabaseRecordTypes.split(",");

const adminDefaultDto: ImportOptions = {
    IncludeExpired: true,
    IncludeArtificial: false,
    IncludeArchived: true,
    TransformScript: "",
    RemoveAnalyzers: false,
    EncryptionKey: undefined,
    OperateOnTypes: adminDefaultOperateOnTypes.join(",") as DatabaseItemType,
    OperateOnDatabaseRecordTypes: "None",
    Collections: null,
    MaxReadOpsPerSecond: null,
} as ImportOptions;

const enterpriseAiOnlyFlags: Partial<LicenseStatus> = {
    HasGenAi: false,
    HasAiAgent: false,
};

const enterpriseOnlyFlags: Partial<LicenseStatus> = {
    HasEmbeddingsGeneration: false,
    HasPullReplicationAsHub: false,
    HasElasticSearchEtl: false,
    HasQueueEtl: false,
    HasSnowflakeEtl: false,
    HasOlapEtl: false,
    HasQueueSink: false,
    HasCdcSink: false,
    HasRemoteAttachments: false,
    HasDocumentsCompression: false,
    HasDataArchival: false,
    HasPostgreSqlIntegration: false,
};

const professionalOnlyFlags: Partial<LicenseStatus> = {
    HasExternalReplication: false,
    HasPullReplicationAsSink: false,
    HasPeriodicBackup: false,
    HasRavenEtl: false,
    HasSqlEtl: false,
    CanSetupDefaultRevisionsConfiguration: false,
    HasTimeSeriesRollupsAndRetention: false,
    HasClientConfiguration: false,
    HasSchemaValidation: false,
};

const licenseTiers: Partial<Record<LicenseType, Partial<LicenseStatus>>> = {
    Community: { ...enterpriseAiOnlyFlags, ...enterpriseOnlyFlags, ...professionalOnlyFlags },
    Professional: { ...enterpriseAiOnlyFlags, ...enterpriseOnlyFlags },
    Enterprise: enterpriseAiOnlyFlags,
};

const enterpriseDefaultRecordTypes: DatabaseRecordItemType[] = [
    ...allSettingTokens,
    ...without(allOngoingTaskTokens, "GenAiEtls", "AiAgents"),
    // AI connection strings stay: embeddings generation (Enterprise) still uses them
    ...allConnectionStringTokens,
];

const professionalDefaultRecordTypes: DatabaseRecordItemType[] = [
    "Settings",
    "ConflictSolverConfig",
    "Client",
    "Revisions",
    "Refresh",
    "Expiration",
    "SchemaValidation",
    "TimeSeries",
    "Sorters",
    "Analyzers",
    "PeriodicBackups",
    "ExternalReplications",
    "RavenEtls",
    "SqlEtls",
    "SinkPullReplications",
    "RavenConnectionStrings",
    "SqlConnectionStrings",
];

const communityDefaultRecordTypes: DatabaseRecordItemType[] = [
    "Settings",
    "ConflictSolverConfig",
    "Refresh",
    "Expiration",
    "Sorters",
    "Analyzers",
];

const tasksMocks = () => ({
    importDatabaseFromFile: jest.mocked(mockServices.tasksService.mock.importDatabaseFromFile),
    validateSmugglerOptions: jest.mocked(mockServices.tasksService.mock.validateSmugglerOptions),
    getNextOperationId: jest.mocked(mockServices.tasksService.mock.getNextOperationId),
});

async function selectFile({ screen, user }: Rendered, name = "dump.ravendbdump") {
    const file = new File(["dump"], name);
    await act(async () => {
        await user.upload(screen.getByTestId(selectors.fileInput), file);
    });
    return file;
}

const toggle = ({ screen, fireClick }: Rendered, label: string | RegExp) =>
    fireClick(screen.getByRole("checkbox", { name: label }));

async function clickImport({ screen, fireClick }: Rendered) {
    const button = screen.getByRole("button", { name: selectors.importButton });
    await waitFor(() => expect(button).toBeEnabled());
    await fireClick(button);
}

async function importAndGetDto(rendered: Rendered): Promise<ImportOptions> {
    await clickImport(rendered);
    const { importDatabaseFromFile } = tasksMocks();
    await waitFor(() => expect(importDatabaseFromFile).toHaveBeenCalledTimes(1));
    return importDatabaseFromFile.mock.calls[0][3];
}

async function expectImportRejected(rendered: Rendered) {
    await clickImport(rendered);
    const { importDatabaseFromFile, validateSmugglerOptions } = tasksMocks();
    await act(async () => {
        await Promise.resolve();
    });
    expect(validateSmugglerOptions).not.toHaveBeenCalled();
    expect(importDatabaseFromFile).not.toHaveBeenCalled();
}

const getSwitch = ({ screen }: Rendered, label: string | RegExp) => screen.getByRole("checkbox", { name: label });

function expectLocked(rendered: Rendered, labels: (string | RegExp)[]) {
    labels.forEach((label) => {
        const input = getSwitch(rendered, label);
        expect(input).toBeDisabled();
        expect(input).not.toBeChecked();
    });
}

function expectUsable(rendered: Rendered, labels: (string | RegExp)[]) {
    labels.forEach((label) => {
        const input = getSwitch(rendered, label);
        expect(input).toBeEnabled();
        expect(input).toBeChecked();
    });
}

describe("ImportDatabaseFromFile", () => {
    beforeEach(() => {
        const { importDatabaseFromFile, validateSmugglerOptions, getNextOperationId } = tasksMocks();
        importDatabaseFromFile.mockClear();
        validateSmugglerOptions.mockClear();
        getNextOperationId.mockClear();
    });

    describe("file selection", () => {
        it("keeps both action buttons disabled until a file is selected", async () => {
            const rendered = rtlRender(<Default />);
            const { screen } = rendered;

            expect(screen.getByRole("button", { name: selectors.importButton })).toBeDisabled();
            expect(screen.getByRole("button", { name: selectors.commandButton })).toBeDisabled();

            await selectFile(rendered);

            await waitFor(() => expect(screen.getByRole("button", { name: selectors.importButton })).toBeEnabled());
            expect(screen.getByRole("button", { name: selectors.commandButton })).toBeEnabled();
        });

        it("uploads the selected file together with the options", async () => {
            const rendered = rtlRender(<Default />);
            const file = await selectFile(rendered);

            await importAndGetDto(rendered);

            const { importDatabaseFromFile, validateSmugglerOptions, getNextOperationId } = tasksMocks();
            expect(validateSmugglerOptions).toHaveBeenCalledTimes(1);
            expect(getNextOperationId).toHaveBeenCalledTimes(1);
            expect(importDatabaseFromFile.mock.calls[0][2]).toBe(file);
        });

        it("rejects a snapshot file and never calls the server", async () => {
            const rendered = rtlRender(<Default />);
            await selectFile(rendered, "backup.ravendb-snapshot");

            await expectImportRejected(rendered);

            expect(rendered.screen.getByText(/RavenDB Snapshot file/)).toBeInTheDocument();
        });
    });

    describe("default DTO", () => {
        it("sends the admin defaults exactly when nothing is changed", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            await selectFile(rendered);

            const dto = await importAndGetDto(rendered);

            expect(dto).toEqual(adminDefaultDto);
        });

        it("omits Indexes and admin-only settings for a read-write user", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures databaseAccess="DatabaseReadWrite" />);
            await selectFile(rendered);

            const dto = await importAndGetDto(rendered);

            expect(operateOnTypesOf(dto)).toEqual(without(adminDefaultOperateOnTypes, "Indexes"));
            expect(recordTypesOf(dto)).toEqual([
                "Settings",
                "ConflictSolverConfig",
                "Refresh",
                "DocumentsCompression",
                "SchemaValidation",
                "DataArchival",
                "TimeSeries",
                "Sorters",
                "Analyzers",
                "PostgreSQLIntegration",
            ]);
            expect(rendered.screen.getByText(selectors.accessAlert)).toBeInTheDocument();
        });
    });

    describe("default DTO per license tier", () => {
        it("Enterprise AI / Developer: every feature licensed - record types collapse to None", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures licenseType="EnterpriseAi" />);
            await selectFile(rendered);

            const dto = await importAndGetDto(rendered);

            expect(recordTypesOf(dto)).toEqual(["None"]);
            expect(dto.IncludeArchived).toBe(true);
            expect(rendered.screen.queryByText(selectors.licenseAlert)).not.toBeInTheDocument();
        });

        it("Enterprise: emits everything except the AI-only tasks", async () => {
            const rendered = rtlRender(
                <Default hasAllLicenseFeatures licenseType="Enterprise" licenseOverrides={licenseTiers.Enterprise} />
            );
            await selectFile(rendered);

            const dto = await importAndGetDto(rendered);

            expect(recordTypesOf(dto)).toEqual(enterpriseDefaultRecordTypes);
            expect(dto.IncludeArchived).toBe(true);
            expect(rendered.screen.getByText(selectors.licenseAlert)).toBeInTheDocument();
        });

        it("Professional: emits only the Professional+ settings, tasks and connection strings", async () => {
            const rendered = rtlRender(
                <Default
                    hasAllLicenseFeatures
                    licenseType="Professional"
                    licenseOverrides={licenseTiers.Professional}
                />
            );
            await selectFile(rendered);

            const dto = await importAndGetDto(rendered);

            expect(recordTypesOf(dto)).toEqual(professionalDefaultRecordTypes);
            expect(dto.IncludeArchived).toBe(false);
            expect(rendered.screen.getByText(selectors.licenseAlert)).toBeInTheDocument();
        });

        it("Community: emits only the ungated settings - no tasks, no connection strings", async () => {
            const rendered = rtlRender(
                <Default hasAllLicenseFeatures licenseType="Community" licenseOverrides={licenseTiers.Community} />
            );
            await selectFile(rendered);

            const dto = await importAndGetDto(rendered);

            expect(recordTypesOf(dto)).toEqual(communityDefaultRecordTypes);
            expect(dto.IncludeArchived).toBe(false);
            expect(operateOnTypesOf(dto)).toEqual(adminDefaultOperateOnTypes);
            expect(rendered.screen.getByText(selectors.licenseAlert)).toBeInTheDocument();
        });

        it("keeps restricted tasks out after Reset to default re-enables 'include all tasks'", async () => {
            const rendered = rtlRender(
                <Default
                    hasAllLicenseFeatures
                    licenseType="Professional"
                    licenseOverrides={licenseTiers.Professional}
                />
            );
            await selectFile(rendered);

            await rendered.fireClick(rendered.screen.getByRole("button", { name: /Reset to default/ }));

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual(professionalDefaultRecordTypes);
        });

        it("expands to the server defaults minus restricted when the user asks for everything", async () => {
            const rendered = rtlRender(
                <Default
                    hasAllLicenseFeatures
                    licenseType="Professional"
                    licenseOverrides={licenseTiers.Professional}
                />
            );
            const { screen, fireClick } = rendered;
            await selectFile(rendered);

            await fireClick(screen.getByRole("button", { name: /Reset to default/ }));
            await fireClick(screen.getByRole("button", { name: /Import all settings/ }));

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual([
                ...professionalDefaultRecordTypes,
                "LockMode",
                "QueueSinks",
                "IndexesHistory",
            ]);
        });

        it("re-applies the gating when the license status arrives after mount", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures licenseType="EnterpriseAi" />);
            await selectFile(rendered);

            await act(async () => {
                mockStore.license.with_License({ Type: "Community", ...licenseTiers.Community });
            });

            const dto = await importAndGetDto(rendered);

            expect(recordTypesOf(dto)).toEqual(communityDefaultRecordTypes);
            expect(dto.IncludeArchived).toBe(false);
        });
    });

    describe("locking and restrictions in the UI", () => {
        it("keeps every option locked until a file is selected", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);

            expect(getSwitch(rendered, "Include Documents")).toBeDisabled();
            expect(getSwitch(rendered, "Include Indexes")).toBeDisabled();
            expect(getSwitch(rendered, /Use transform script/)).toBeDisabled();

            await selectFile(rendered);

            expect(getSwitch(rendered, "Include Documents")).toBeEnabled();
            expect(getSwitch(rendered, "Include Indexes")).toBeEnabled();
            expect(getSwitch(rendered, /Use transform script/)).toBeEnabled();
        });

        it("Community: locks the licensed document toggle and settings rows, leaves the rest usable", async () => {
            const rendered = rtlRender(
                <Default hasAllLicenseFeatures licenseType="Community" licenseOverrides={licenseTiers.Community} />
            );
            await selectFile(rendered);

            expectLocked(rendered, ["Include Archived Documents"]);
            expectUsable(rendered, ["Include Expired Documents", "Include Documents"]);

            expectLocked(rendered, [
                "Client Configuration",
                "Revisions Configuration",
                "Documents Compression",
                "Document Schema",
                "Data Archival",
                "Time Series Configuration",
                "PostgreSQL Integration",
            ]);
            expectUsable(rendered, [
                "Settings",
                "Conflict Solver Configuration",
                "Document Refresh",
                "Document Expiration",
                "Custom Sorters",
                "Custom Analyzers",
            ]);
        });

        it("Professional: locks the Enterprise-only tasks and connection strings in the customize panel", async () => {
            const rendered = rtlRender(
                <Default
                    hasAllLicenseFeatures
                    licenseType="Professional"
                    licenseOverrides={licenseTiers.Professional}
                />
            );
            const { screen } = rendered;
            await selectFile(rendered);

            expect(screen.getByRole("button", { name: /Reset to default/ })).toBeInTheDocument();

            expectLocked(rendered, [
                "Snowflake ETLs",
                "OLAP ETLs",
                "Elasticsearch ETLs",
                /^Queue ETLs/,
                "Replication Hubs",
                "Embeddings Generation",
                "GenAI",
                "CDC Sinks",
                "AI Agents",
                "Remote Attachments",
            ]);
            expectUsable(rendered, [
                "Periodic Backups",
                "External Replications",
                "RavenDB ETLs",
                "SQL ETLs",
                "Replication Sinks",
            ]);

            expectLocked(rendered, [
                "Snowflake Connection Strings",
                "OLAP Connection Strings",
                "Elasticsearch Connection Strings",
                /^Queue Connection Strings/,
                "AI Connection Strings",
            ]);
            expectUsable(rendered, ["RavenDB Connection Strings", "SQL Connection Strings"]);

            expect(screen.getAllByTestId("license-restricted-badge")).toHaveLength(19);
        });

        it("locks only the rows of a single missing feature and drops just those from the DTO", async () => {
            const rendered = rtlRender(
                <Default hasAllLicenseFeatures licenseType="Enterprise" licenseOverrides={{ HasSqlEtl: false }} />
            );
            const { screen } = rendered;
            await selectFile(rendered);

            expectLocked(rendered, ["SQL ETLs", "SQL Connection Strings"]);
            expectUsable(rendered, ["RavenDB ETLs", "Snowflake ETLs", "RavenDB Connection Strings"]);
            expect(screen.getAllByTestId("license-restricted-badge")).toHaveLength(2);

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual(
                without(
                    [...allSettingTokens, ...allOngoingTaskTokens, ...allConnectionStringTokens],
                    "SqlEtls",
                    "SqlConnectionStrings"
                )
            );
        });

        it("sharded database: locks the tasks the shards cannot run and PostgreSQL, keeps connection strings", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures isSharded />);
            const { screen } = rendered;
            await selectFile(rendered);

            expectLocked(rendered, [
                /^Queue ETLs/,
                "Replication Hubs",
                "Replication Sinks",
                "GenAI",
                "CDC Sinks",
                "AI Agents",
                "Remote Attachments",
                "PostgreSQL Integration",
            ]);
            expectUsable(rendered, [
                "Periodic Backups",
                "RavenDB ETLs",
                "Elasticsearch ETLs",
                "Embeddings Generation",
                /^Queue Connection Strings/,
                "AI Connection Strings",
                "Documents Compression",
            ]);

            expect(screen.queryAllByTestId("license-restricted-badge")).toHaveLength(0);
            expect(screen.getByText(selectors.accessAlert)).toBeInTheDocument();
            expect(screen.queryByText(selectors.licenseAlert)).not.toBeInTheDocument();

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual([
                ...without(allSettingTokens, "PostgreSQLIntegration"),
                ...without(
                    allOngoingTaskTokens,
                    "QueueEtls",
                    "HubPullReplications",
                    "SinkPullReplications",
                    "GenAiEtls",
                    "CdcSinks",
                    "AiAgents",
                    "RemoteAttachments"
                ),
                ...allConnectionStringTokens,
            ]);
        });

        it("read-write access: locks every task and connection string without a license badge", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures databaseAccess="DatabaseReadWrite" />);
            const { screen } = rendered;
            await selectFile(rendered);

            expectLocked(rendered, [
                "Periodic Backups",
                "RavenDB ETLs",
                "Replication Hubs",
                "RavenDB Connection Strings",
            ]);
            expect(getSwitch(rendered, "Select all ongoing tasks")).toBeDisabled();
            expect(getSwitch(rendered, "Select all connection strings")).toBeDisabled();
            expect(screen.queryAllByTestId("license-restricted-badge")).toHaveLength(0);
        });
    });

    describe("documents and extensions", () => {
        it("drops Documents and cascades Attachments off when documents are excluded", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            await selectFile(rendered);

            await toggle(rendered, "Include Documents");

            expect(rendered.screen.getByRole("checkbox", { name: "Include Attachments" })).not.toBeChecked();
            const dto = await importAndGetDto(rendered);
            expect(operateOnTypesOf(dto)).toEqual(without(adminDefaultOperateOnTypes, "Documents", "Attachments"));
        });

        it("maps the three document flags one to one", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            await selectFile(rendered);

            await toggle(rendered, /Include Artificial Documents/);
            await toggle(rendered, "Include Archived Documents");
            await toggle(rendered, "Include Expired Documents");

            const dto = await importAndGetDto(rendered);
            expect(dto.IncludeArtificial).toBe(true);
            expect(dto.IncludeArchived).toBe(false);
            expect(dto.IncludeExpired).toBe(false);
            expect(operateOnTypesOf(dto)).toEqual(adminDefaultOperateOnTypes);
        });

        it("sends the hand-typed collection list when collections are customized", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            const { screen, fireClick, fillInput } = rendered;
            await selectFile(rendered);

            await fireClick(screen.getByRole("button", { name: /Customize imported collections/ }));
            await fillInput(screen.getByPlaceholderText("Type a collection name from the imported file"), "Orders");
            await fireClick(screen.getByRole("button", { name: /Add/ }));
            await waitFor(() => expect(screen.getByRole("checkbox", { name: "Orders" })).toBeChecked());

            const dto = await importAndGetDto(rendered);
            expect(dto.Collections).toEqual(["Orders"]);
        });
    });

    describe("configuration", () => {
        it("requests IndexesHistory alone when index history is the only customization", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            await selectFile(rendered);

            await toggle(rendered, "Include Index History");

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual(["IndexesHistory"]);
            expect(operateOnTypesOf(dto)).toEqual(adminDefaultOperateOnTypes);
        });

        it("turns Remove Analyzers into a flag and Indexes off cascades it back", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            const { screen } = rendered;
            await selectFile(rendered);

            await toggle(rendered, "Remove Analyzers");
            expect(screen.getByRole("checkbox", { name: "Remove Analyzers" })).toBeChecked();

            await toggle(rendered, "Include Indexes");
            expect(screen.getByRole("checkbox", { name: "Remove Analyzers" })).not.toBeChecked();

            const dto = await importAndGetDto(rendered);
            expect(dto.RemoveAnalyzers).toBe(false);
            expect(operateOnTypesOf(dto)).toEqual(without(adminDefaultOperateOnTypes, "Indexes"));
        });

        it("lists every setting explicitly - never None - when tasks are excluded", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            await selectFile(rendered);

            await toggle(rendered, /Include Connection Strings & Ongoing Tasks/);

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual(allSettingTokens);
        });

        it("emits only the checked tasks once ongoing tasks are customized", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            const { screen, fireClick } = rendered;
            await selectFile(rendered);

            await fireClick(screen.getAllByRole("button", { name: /^Customize$/ })[0]);
            await fireClick(screen.getByRole("checkbox", { name: "Select all ongoing tasks" }));
            await toggle(rendered, "RavenDB ETLs");

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual([...allSettingTokens, "RavenEtls", ...allConnectionStringTokens]);
        });

        it("returns to None after Reset to default", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            const { screen, fireClick } = rendered;
            await selectFile(rendered);

            await fireClick(screen.getAllByRole("button", { name: /^Customize$/ })[0]);
            await toggle(rendered, "RavenDB ETLs");
            await fireClick(screen.getByRole("button", { name: /Reset to default/ }));

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual(["None"]);
        });

        it("emits only the checked settings once settings are customized", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            const { screen, fireClick } = rendered;
            await selectFile(rendered);

            await fireClick(screen.getAllByRole("button", { name: /^Customize$/ })[1]);
            await fireClick(screen.getByRole("checkbox", { name: "Select all database settings" }));
            await toggle(rendered, "Custom Sorters");

            const dto = await importAndGetDto(rendered);
            expect(recordTypesOf(dto)).toEqual(["Sorters", ...allOngoingTaskTokens, ...allConnectionStringTokens]);
        });

        it("blocks the import when nothing is left to include", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            const { screen, fireClick } = rendered;
            await selectFile(rendered);

            // artificial / legacy / index history default off, so the bulk buttons start as "Select all":
            // the first click turns everything on and flips them to "Deselect all"
            const [documentsBulk, entitiesBulk] = screen.getAllByRole("button", { name: "Select all" });
            await fireClick(documentsBulk);
            expect(documentsBulk).toHaveTextContent("Deselect all");
            await fireClick(documentsBulk);
            await fireClick(entitiesBulk);
            expect(entitiesBulk).toHaveTextContent("Deselect all");
            await fireClick(entitiesBulk);
            await fireClick(screen.getAllByRole("button", { name: /^Customize$/ })[1]);
            await fireClick(screen.getByRole("checkbox", { name: "Select all database settings" }));

            expect(screen.getByText(selectors.noIncludeAlert)).toBeInTheDocument();
            expect(screen.getByRole("button", { name: selectors.importButton })).toBeDisabled();
            expect(tasksMocks().importDatabaseFromFile).not.toHaveBeenCalled();
        });
    });

    describe("processing", () => {
        it("sends the transform script only while the toggle is on", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            await selectFile(rendered);

            await toggle(rendered, /Use transform script/);

            const dto = await importAndGetDto(rendered);
            expect(dto.TransformScript).toBe(defaultTransformScript);
            expect(tasksMocks().validateSmugglerOptions.mock.calls[0][0]).toEqual({
                TransformScript: defaultTransformScript,
            });
        });

        it("casts the max read ops input to a number", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            const { screen, fillInput } = rendered;
            await selectFile(rendered);

            await toggle(rendered, "Set max read operations per second");
            await fillInput(screen.getByPlaceholderText("Max read operations per second"), "500");

            const dto = await importAndGetDto(rendered);
            expect(dto.MaxReadOpsPerSecond).toBe(500);
        });

        it("refuses to import with max read ops enabled but empty", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            await selectFile(rendered);

            await toggle(rendered, "Set max read operations per second");

            await expectImportRejected(rendered);
            expect(rendered.screen.getByText("Value is required")).toBeInTheDocument();
        });

        it("sends the encryption key only when the file is marked encrypted", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            const { screen, fillInput } = rendered;
            await selectFile(rendered);

            await toggle(rendered, "Imported file is encrypted");
            await fillInput(screen.getByPlaceholderText("Key"), "c2VjcmV0");

            const dto = await importAndGetDto(rendered);
            expect(dto.EncryptionKey).toBe("c2VjcmV0");
        });

        it("refuses to import an encrypted file without a key", async () => {
            const rendered = rtlRender(<Default hasAllLicenseFeatures />);
            await selectFile(rendered);

            await toggle(rendered, "Imported file is encrypted");

            await expectImportRejected(rendered);
            expect(rendered.screen.getByText("Encryption key is required")).toBeInTheDocument();
        });
    });
});
