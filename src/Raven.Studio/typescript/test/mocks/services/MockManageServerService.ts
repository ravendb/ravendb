import ManageServerService from "components/services/ManageServerService";
import { AutoMockService, MockedValue } from "./AutoMockService";
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import AnalyzerDefinition = Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;
import SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;
import { SharedStubs } from "test/stubs/SharedStubs";
import { mockJQueryError } from "test/mocks/utils";
import { DebugPackageStubs } from "test/stubs/DebugPackageStubs";

type RaftDebugView = Raven.Server.Rachis.RaftDebugView;
type OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
type SettingsResult = Raven.Server.Config.SettingsResult;

export default class MockManageServerService extends AutoMockService<ManageServerService> {
    constructor() {
        super(new ManageServerService());
    }

    withGetGlobalClientConfiguration(dto?: MockedValue<ClientConfiguration>) {
        return this.mockResolvedValue(
            this.mocks.getGlobalClientConfiguration,
            dto,
            ManageServerStubs.getSampleClientGlobalConfiguration()
        );
    }

    withThrowingGetGlobalClientConfiguration() {
        this.mocks.getGlobalClientConfiguration.mockRejectedValue(new Error());
    }

    withGetDatabaseClientConfiguration(dto?: MockedValue<ClientConfiguration>) {
        return this.mockResolvedValue(
            this.mocks.getClientConfiguration,
            dto,
            ManageServerStubs.getSampleClientDatabaseConfiguration()
        );
    }

    withServerWideCustomAnalyzers(dto?: MockedValue<AnalyzerDefinition[]>) {
        return this.mockResolvedValue(
            this.mocks.getServerWideCustomAnalyzers,
            dto,
            ManageServerStubs.serverWideCustomAnalyzers()
        );
    }

    withThrowingGetServerWideCustomAnalyzers() {
        this.mocks.getServerWideCustomAnalyzers.mockRejectedValue(new Error());
    }

    withServerWideCustomSorters(dto?: MockedValue<SorterDefinition[]>) {
        return this.mockResolvedValue(
            this.mocks.getServerWideCustomSorters,
            dto,
            ManageServerStubs.serverWideCustomSorters()
        );
    }

    withThrowingGetServerWideCustomSorters() {
        this.mocks.getServerWideCustomSorters.mockRejectedValue(new Error());
    }

    withTestPeriodicBackupCredentials(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testPeriodicBackupCredentials,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withServerSettings(dto?: MockedValue<Raven.Server.Config.SettingsResult>) {
        return this.mockResolvedValue(this.mocks.getServerSettings, dto, ManageServerStubs.serverSettings());
    }

    withGetClusterLog() {
        return this.mocks.getClusterLog.mockImplementation(
            async (nodeTag): Promise<Raven.Server.Rachis.RaftDebugView> => {
                if (nodeTag === "B") {
                    return ManageServerStubs.getClusterLogLeader();
                }
                if (nodeTag === "C") {
                    throw mockJQueryError("This is an error message");
                }

                return ManageServerStubs.getClusterLogFollower();
            }
        );
    }

    withGetClusterLogEntry() {
        return this.mockResolvedValue(this.mocks.getClusterLogEntry, null, ManageServerStubs.getClusterLogEntry());
    }

    withAdminLogsConfiguration(dto?: MockedValue<Raven.Client.ServerWide.Operations.Logs.GetLogsConfigurationResult>) {
        return this.mockResolvedValue(
            this.mocks.getAdminLogsConfiguration,
            dto,
            ManageServerStubs.adminLogsConfiguration()
        );
    }

    withEventListenerConfiguration(
        dto?: MockedValue<Omit<Raven.Server.EventListener.EventListenerToLog.EventListenerConfiguration, "Persist">>
    ) {
        return this.mockResolvedValue(
            this.mocks.getEventListenerConfiguration,
            dto,
            ManageServerStubs.eventListenerConfiguration()
        );
    }

    withTrafficWatchConfiguration(
        dto?: MockedValue<
            Omit<
                Raven.Client.ServerWide.Operations.TrafficWatch.PutTrafficWatchConfigurationOperation.Parameters,
                "Persist"
            >
        >
    ) {
        return this.mockResolvedValue(
            this.mocks.getTrafficWatchConfiguration,
            dto,
            ManageServerStubs.trafficWatchConfiguration()
        );
    }

    withCertificates(dto?: MockedValue<CertificatesResponseDto>) {
        return this.mockResolvedValue(this.mocks.getCertificates, dto, ManageServerStubs.certificates());
    }

    withAdminStats(dto?: MockedValue<Raven.Server.ServerWide.ServerStatistics>) {
        return this.mockResolvedValue(this.mocks.getAdminStats, dto, ManageServerStubs.adminStats());
    }

    withServerCertificateRenewalDate(dto?: MockedValue<string>) {
        return this.mockResolvedValue(
            this.mocks.getServerCertificateRenewalDate,
            dto,
            ManageServerStubs.serverCertificateRenewalDate()
        );
    }

    withServerCertificateSetupMode(dto?: MockedValue<Raven.Server.Commercial.SetupMode>) {
        return this.mockResolvedValue(
            this.mocks.getServerCertificateSetupMode,
            dto,
            ManageServerStubs.serverCertificateSetupMode()
        );
    }

    withGenerateTwoFactorSecret(dto?: MockedValue<{ Secret: string }>) {
        return this.mockResolvedValue(this.mocks.generateTwoFactorSecret, dto, ManageServerStubs.twoFactorSecret());
    }

    // ----- Debug Package Analyzer (on-demand section data) -----
    // Each helper resolves a filled stub by default; pass `empty: true` to resolve the no-data value
    // so the corresponding section in the story renders its empty state.

    private mockDebugPackageValue<T>(mock: any, empty: boolean, emptyValue: T, filledValue: T): T {
        const value = empty ? emptyValue : filledValue;
        mock.mockResolvedValue(value);
        return value;
    }

    withDebugPackageClusterLog(empty = false) {
        return this.mocks.getDebugPackageClusterLog.mockImplementation(
            async (_packageId: string, nodeTag: string): Promise<RaftDebugView> => {
                if (empty) {
                    return null as unknown as RaftDebugView;
                }
                return nodeTag === "A"
                    ? ManageServerStubs.getClusterLogLeader()
                    : ManageServerStubs.getClusterLogFollower();
            }
        );
    }

    withDebugPackageClusterObserverDecisions(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageClusterObserverDecisions,
            empty,
            null,
            DebugPackageStubs.observerDecisions()
        );
    }

    withDebugPackageDatabaseStats(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageDatabaseStats,
            empty,
            null,
            DebugPackageStubs.databaseStats()
        );
    }

    withDebugPackageDatabaseIndexStats(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageDatabaseIndexStats,
            empty,
            [],
            DebugPackageStubs.databaseIndexStats()
        );
    }

    withDebugPackageDatabaseIndexDefinitions(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageDatabaseIndexDefinitions,
            empty,
            [],
            DebugPackageStubs.databaseIndexDefinitions()
        );
    }

    withDebugPackageDatabaseIndexErrors(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageDatabaseIndexErrors,
            empty,
            [],
            DebugPackageStubs.databaseIndexErrors()
        );
    }

    withDebugPackageDatabaseOngoingTasks(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageDatabaseOngoingTasks,
            empty,
            { OngoingTasks: [] } as unknown as OngoingTasksResult,
            DebugPackageStubs.databaseOngoingTasks()
        );
    }

    withDebugPackageDatabaseSettings(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageDatabaseSettings,
            empty,
            { Settings: [] } as unknown as SettingsResult,
            DebugPackageStubs.databaseSettings()
        );
    }

    withDebugPackageNetworkInfo(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageNetworkInfo,
            empty,
            null,
            DebugPackageStubs.networkInfo()
        );
    }

    withDebugPackageThreadsInfo(empty = false) {
        return this.mockDebugPackageValue(
            this.mocks.getDebugPackageThreadsInfo,
            empty,
            null,
            DebugPackageStubs.threadsInfo()
        );
    }
}
