using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup;

public class MaxReadOpsPerSecOptionTests : ClusterTestBase
{
    private readonly ITestOutputHelper _output;

    public MaxReadOpsPerSecOptionTests(ITestOutputHelper output) : base(output)
    {
        _output = output;
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup, BackupScope.Periodic, BackupKind.Full, ConfigurationSource.BackupConfiguration])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup, BackupScope.Periodic, BackupKind.Full, ConfigurationSource.DatabaseSettings])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup, BackupScope.Periodic, BackupKind.Incremental, ConfigurationSource.BackupConfiguration])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup, BackupScope.Periodic, BackupKind.Incremental, ConfigurationSource.DatabaseSettings])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup, BackupScope.ServerWide, BackupKind.Full, ConfigurationSource.BackupConfiguration])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup, BackupScope.ServerWide, BackupKind.Full, ConfigurationSource.DatabaseSettings])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup, BackupScope.ServerWide, BackupKind.Incremental, ConfigurationSource.BackupConfiguration])]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup, BackupScope.ServerWide, BackupKind.Incremental, ConfigurationSource.DatabaseSettings])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot, BackupScope.Periodic, BackupKind.Full, ConfigurationSource.BackupConfiguration])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot, BackupScope.Periodic, BackupKind.Full, ConfigurationSource.DatabaseSettings])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot, BackupScope.Periodic, BackupKind.Incremental, ConfigurationSource.BackupConfiguration])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot, BackupScope.Periodic, BackupKind.Incremental, ConfigurationSource.DatabaseSettings])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot, BackupScope.ServerWide, BackupKind.Full, ConfigurationSource.BackupConfiguration])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot, BackupScope.ServerWide, BackupKind.Full, ConfigurationSource.DatabaseSettings])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot, BackupScope.ServerWide, BackupKind.Incremental, ConfigurationSource.BackupConfiguration])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot, BackupScope.ServerWide, BackupKind.Incremental, ConfigurationSource.DatabaseSettings])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot, BackupScope.Periodic, BackupKind.Full, ConfigurationSource.BackupConfiguration], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot, BackupScope.Periodic, BackupKind.Full, ConfigurationSource.DatabaseSettings], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot, BackupScope.Periodic, BackupKind.Incremental, ConfigurationSource.BackupConfiguration], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot, BackupScope.Periodic, BackupKind.Incremental, ConfigurationSource.DatabaseSettings], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot, BackupScope.ServerWide, BackupKind.Full, ConfigurationSource.BackupConfiguration], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot, BackupScope.ServerWide, BackupKind.Full, ConfigurationSource.DatabaseSettings], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot, BackupScope.ServerWide, BackupKind.Incremental, ConfigurationSource.BackupConfiguration], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot, BackupScope.ServerWide, BackupKind.Incremental, ConfigurationSource.DatabaseSettings], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    public async Task ShouldRespect_Option_MaxReadOpsPerSec_OnPeriodicBackup(Options options, BackupType backupType, BackupScope backupScope, BackupKind backupKind, ConfigurationSource configurationSource)
    {
        DoNotReuseServer();

        using var scenario = new MaxReadOpsPerSecOptionBackupTestScenario(this, options)
        {
            BackupType = backupType,
            BackupScope = backupScope,
            BackupKind = backupKind,
            ConfigurationSource = configurationSource
        };

        await scenario.ConfigurePeriodicBackupSettingsBasedOnScopeAsync();
        await scenario.CreateInitialDocumentsAsync();

        var backupDurationWithDefaults = await scenario.MeasurePeriodicBackupDurationAsync();
        await scenario.SetMaxReadOpsPerSecondAsync();
        var backupDurationWithMaxReadOps = await scenario.MeasurePeriodicBackupDurationAsync();

        scenario.AssertResults(backupDurationWithDefaults, backupDurationWithMaxReadOps);
        _output.WriteLine($"Backup duration with defaults: {backupDurationWithDefaults}, Backup duration with MaxReadOpsPerSec: {backupDurationWithMaxReadOps}");
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot], Skip = "Backups of the type 'Snapshot' are not supported in sharding.")]
    public async Task ShouldRespect_Option_MaxReadOpsPerSec_OnOneTimeBackup(Options options, BackupType backupType)
    {
        DoNotReuseServer();

        using var scenario = new MaxReadOpsPerSecOptionBackupTestScenario(this, options)
        {
            BackupType = backupType,
        };

        scenario.ConfigureOneTimeBackupSettings();
        await scenario.CreateInitialDocumentsAsync();

        var backupDurationWithDefaults = await scenario.MeasureOneTimeBackupDurationAsync();
        scenario.OneTimeBackupConfiguration.MaxReadOpsPerSecond = scenario.MaxReadOpsPerSecToTest;
        var backupDurationWithMaxReadOps = await scenario.MeasureOneTimeBackupDurationAsync();

        scenario.AssertResults(backupDurationWithDefaults, backupDurationWithMaxReadOps);
        _output.WriteLine($"Backup duration with defaults: {backupDurationWithDefaults}, Backup duration with MaxReadOpsPerSec: {backupDurationWithMaxReadOps}");
    }

    [RavenTheory(RavenTestCategory.Smuggler | RavenTestCategory.BackupExportImport)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All, Data = [BackupType.Backup])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single, Data = [BackupType.Snapshot])]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Data = [BackupType.Snapshot], Skip = "Backups of type 'Snapshot' are not supported in sharding.")]
    public async Task ShouldRespect_Option_MaxReadOpsPerSec_OnRestore(Options options, BackupType backupType)
    {
        DoNotReuseServer();

        using var scenario = new MaxReadOpsPerSecOptionRestoreTestScenario(this, options) { BackupType = backupType };

        await scenario.CreateInitialDocumentsAsync();
        await scenario.PrepareBackupFileToRestore();

        var restoreDurationWithDefaults = scenario.MeasureShardedDatabaseRestoreDuration();
        scenario.SetMaxReadOpsPerSecond();
        var restoreDurationWithMaxReadOps = scenario.MeasureShardedDatabaseRestoreDuration();

        scenario.AssertResults(restoreDurationWithDefaults, restoreDurationWithMaxReadOps);
        _output.WriteLine($"Restore duration with defaults: {restoreDurationWithDefaults}, Restore duration with MaxReadOpsPerSec: {restoreDurationWithMaxReadOps}");
    }

    public enum BackupScope
    {
        Periodic,
        ServerWide
    }

    public enum ConfigurationSource
    {
        BackupConfiguration,
        DatabaseSettings
    }

    private abstract class MaxReadOpsPerSecOptionTestScenarioBase : IDisposable
    {
        protected const string InitialDocumentId = "foo/bar";
        protected readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(1);

        protected readonly ClusterTestBase ClusterTestBase;
        protected readonly string BackupPath;
        protected readonly DocumentStore Store;

        private readonly Options _options;
        private int? _expectedMinimumOperationDurationInSeconds;

        public RavenDatabaseMode DatabaseMode => _options.DatabaseMode;

        protected MaxReadOpsPerSecOptionTestScenarioBase(ClusterTestBase clusterTestBase, Options options, [CallerMemberName] string testName = null)
        {
            ClusterTestBase = clusterTestBase;
            _options = options;

            BackupPath = ClusterTestBase.NewDataPath(prefix: testName, suffix: "BackupFolder");
            Store = ClusterTestBase.GetDocumentStore(options, caller: testName);
        }

        protected internal BackupType BackupType { get; init; }
        protected abstract int DocumentsToCreate { get; }
        protected internal abstract int MaxReadOpsPerSecToTest { get; }

        private int ExpectedMinimumOperationDurationInSeconds
        {
            get
            {
                _expectedMinimumOperationDurationInSeconds ??= CalculateExpectedMinimumOperationDurationInSeconds();
                return _expectedMinimumOperationDurationInSeconds.Value;
            }
        }

        protected abstract int CalculateExpectedMinimumOperationDurationInSeconds();

        protected abstract string OperationName { get; }

        protected async Task CreateDocumentsForNonShardedDatabaseAsync()
        {
            for (int i = 0; i < DocumentsToCreate; i++)
                using (var session = Store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"Name{i}" });
                    await session.SaveChangesAsync();
                }
        }

        internal async Task CreateInitialSingleDocumentAsync()
        {
            using var session = Store.OpenAsyncSession();
            await session.StoreAsync(new User { Name = "InitialDocument" }, InitialDocumentId);
            await session.SaveChangesAsync();
        }

        public void AssertResults(TimeSpan defaultDuration, TimeSpan rateControlledDuration)
        {
            Assert.True(rateControlledDuration > defaultDuration,
                $"{OperationName} with MaxReadOpsPerSecond = {MaxReadOpsPerSecToTest} should take more time than {OperationName} with default value, " +
                $"but it took '{rateControlledDuration}', while with default value took '{defaultDuration}'");

            Assert.True(defaultDuration.TotalSeconds < ExpectedMinimumOperationDurationInSeconds,
                $"{OperationName} with default options should take less than '{ExpectedMinimumOperationDurationInSeconds}' seconds, but it took " +
                $"'{defaultDuration}' despite MaxReadOpsPerSecond was not set");

            Assert.True(rateControlledDuration.TotalSeconds > ExpectedMinimumOperationDurationInSeconds,
                $"{OperationName} with MaxReadOpsPerSecond = {MaxReadOpsPerSecToTest} should take more than " +
                $"'{ExpectedMinimumOperationDurationInSeconds}' seconds, but it took '{rateControlledDuration}'");
        }

        public void Dispose()
        {
            Store?.Dispose();
        }
    }

    private class MaxReadOpsPerSecOptionBackupTestScenario : MaxReadOpsPerSecOptionTestScenarioBase
    {
        private long _taskId;

        public MaxReadOpsPerSecOptionBackupTestScenario(ClusterTestBase clusterTestBase, Options options, [CallerMemberName] string testName = null)
            : base(clusterTestBase, options, testName) { }

        public BackupScope BackupScope { get; init; }
        public BackupKind BackupKind { get; init; } = BackupKind.Full;
        public ConfigurationSource ConfigurationSource { get; init; }

        private PeriodicBackupConfiguration PeriodicBackupConfiguration { get; set; }
        private ServerWideBackupConfiguration ServerWideBackupConfiguration { get; set; }
        internal BackupConfiguration OneTimeBackupConfiguration { get; private set; }

        protected override int DocumentsToCreate => BackupType == BackupType.Snapshot ? 200 : 150;
        protected internal override int MaxReadOpsPerSecToTest => BackupType == BackupType.Snapshot ? 16 : 10;

        protected override string OperationName => $"{BackupKind} {(BackupType == BackupType.Backup ? "Logical" : nameof(BackupType.Snapshot))} backup of {DatabaseMode} database";

        protected override int CalculateExpectedMinimumOperationDurationInSeconds()
        {
            switch (BackupType)
            {
                case BackupType.Snapshot when BackupKind == BackupKind.Full:
                    // Snapshots are created by copying data page by page, rather than one document at a time. Due to specific data storage characteristics,
                    // creating 200 documents results in 257 pages for the document data. (value obtained experimentally)
                    // And minus `MaxReadOpsPerSecToTest` because rateGate will not wait after the last operation
                    return (257 - MaxReadOpsPerSecToTest) / MaxReadOpsPerSecToTest;

                case BackupType.Snapshot when BackupKind == BackupKind.Incremental: // Incremental for snapshot is a logical backup
                case BackupType.Backup:
                    // Minus `MaxReadOpsPerSecToTest` because rateGate will not wait after the last operation
                    return (DocumentsToCreate - MaxReadOpsPerSecToTest) / MaxReadOpsPerSecToTest;

                default:
                    throw new ArgumentOutOfRangeException(nameof(BackupType), BackupType, null);
            }
        }

        public async Task CreateInitialDocumentsAsync()
        {
            switch (DatabaseMode)
            {
                case RavenDatabaseMode.Single when BackupKind == BackupKind.Full:
                    await CreateDocumentsForNonShardedDatabaseAsync();
                    break;

                case RavenDatabaseMode.Sharded when BackupKind == BackupKind.Full:
                    // We want to store all documents in the same shard to get clear understanding the number of documents per shard to do clear measurements
                    await CreateInitialSingleDocumentAsync();
                    await CreateDocumentsOnTheSameShardAsInitialDocumentAsync();
                    break;

                case RavenDatabaseMode.Single  when BackupKind == BackupKind.Incremental:
                    await CreateInitialSingleDocumentAsync();
                    _ = await MeasureNonShardedBackupDurationAsync();
                    break;

                case RavenDatabaseMode.Sharded when BackupKind == BackupKind.Incremental:
                    await CreateInitialSingleDocumentAsync();
                    _ = await MeasureShardedBackupDurationAsync();
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private async Task CreateDocumentsOnTheSameShardAsInitialDocumentAsync()
        {
            var docsToCreate = DocumentsToCreate;
            if (BackupKind == BackupKind.Full)
                docsToCreate -= 1; // We already created the initial document and didn't back it up yet

            for (int i = 1; i < docsToCreate; i++)
                using (var session = Store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"Name{i}" }, $"{nameof(User)}s/{i}${InitialDocumentId}");
                    await session.SaveChangesAsync();
                }
        }

        public async Task ConfigurePeriodicBackupSettingsBasedOnScopeAsync()
        {
            switch (BackupScope)
            {
                case BackupScope.Periodic:
                    PeriodicBackupConfiguration = ClusterTestBase.Backup.CreateBackupConfiguration(BackupPath, BackupType);

                    switch (DatabaseMode)
                    {
                        case RavenDatabaseMode.Single:
                            _taskId = PeriodicBackupConfiguration.TaskId = await ClusterTestBase.Backup.UpdateConfigAsync(ClusterTestBase.Server, PeriodicBackupConfiguration, Store);
                            break;
                        case RavenDatabaseMode.Sharded:
                            _taskId = PeriodicBackupConfiguration.TaskId = await ClusterTestBase.Sharding.Backup.UpdateConfigAsync(ClusterTestBase.Server, PeriodicBackupConfiguration, Store);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(DatabaseMode), DatabaseMode, null);
                    }

                    Assert.Null(ServerWideBackupConfiguration);
                    Assert.NotNull(PeriodicBackupConfiguration);
                    break;

                case BackupScope.ServerWide:
                    ServerWideBackupConfiguration = new ServerWideBackupConfiguration { BackupType = BackupType, FullBackupFrequency = "0 0 1 1 *", Disabled = false, LocalSettings = new LocalSettings { FolderPath = BackupPath } };

                    switch (DatabaseMode)
                    {
                        case RavenDatabaseMode.Single:
                            _taskId = ServerWideBackupConfiguration.TaskId = await ClusterTestBase.Backup.UpdateServerWideConfigAsync(ClusterTestBase.Server, ServerWideBackupConfiguration, Store);
                            break;
                        case RavenDatabaseMode.Sharded:
                            _taskId = ServerWideBackupConfiguration.TaskId = await ClusterTestBase.Sharding.Backup.UpdateServerWideConfigAsync(ClusterTestBase.Server, ServerWideBackupConfiguration, Store);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException(nameof(DatabaseMode), DatabaseMode, null);
                    }

                    Assert.Null(PeriodicBackupConfiguration);
                    Assert.NotNull(ServerWideBackupConfiguration);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(BackupScope), BackupScope, null);
            }
        }

        public void ConfigureOneTimeBackupSettings()
        {
            OneTimeBackupConfiguration = new BackupConfiguration
            {
                BackupType = BackupType,
                LocalSettings = new LocalSettings { FolderPath = BackupPath },
            };
        }

        public async Task SetMaxReadOpsPerSecondAsync()
        {
            if (ConfigurationSource == ConfigurationSource.DatabaseSettings)
            {
                switch (DatabaseMode)
                {
                    case RavenDatabaseMode.Single:
                        var database = await ClusterTestBase.Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(Store.Database);
                        database.Configuration.Backup.MaxReadOpsPerSecond = MaxReadOpsPerSecToTest;


                        break;

                    case RavenDatabaseMode.Sharded:
                        await foreach (var shardedDocumentDatabase in ClusterTestBase.Sharding.GetShardsDocumentDatabaseInstancesFor(Store.Database,[ClusterTestBase.Server]))
                            shardedDocumentDatabase.Configuration.Backup.MaxReadOpsPerSecond = MaxReadOpsPerSecToTest;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException(nameof(DatabaseMode), DatabaseMode, null);
                }

                Assert.Null(PeriodicBackupConfiguration?.MaxReadOpsPerSecond);
                Assert.Null(ServerWideBackupConfiguration?.MaxReadOpsPerSecond);
                return;
            }

            switch (BackupScope)
            {
                case BackupScope.Periodic:
                    Assert.NotNull(PeriodicBackupConfiguration);
                    Assert.Null(ServerWideBackupConfiguration);

                    PeriodicBackupConfiguration.MaxReadOpsPerSecond = MaxReadOpsPerSecToTest;
                    switch (DatabaseMode)
                    {
                        case RavenDatabaseMode.Single:
                            await ClusterTestBase.Backup.UpdateConfigAsync(ClusterTestBase.Server, PeriodicBackupConfiguration, Store);
                            break;

                        case RavenDatabaseMode.Sharded:
                            await ClusterTestBase.Sharding.Backup.UpdateConfigAsync(ClusterTestBase.Server, PeriodicBackupConfiguration, Store);
                            break;

                        default:
                            throw new ArgumentOutOfRangeException(nameof(DatabaseMode), DatabaseMode, null);
                    }
                    break;

                case BackupScope.ServerWide:
                    Assert.NotNull(ServerWideBackupConfiguration);
                    Assert.Null(PeriodicBackupConfiguration);

                    ServerWideBackupConfiguration.MaxReadOpsPerSecond = MaxReadOpsPerSecToTest;
                    switch (DatabaseMode)
                    {
                        case RavenDatabaseMode.Single:
                            await ClusterTestBase.Backup.UpdateServerWideConfigAsync(ClusterTestBase.Server, ServerWideBackupConfiguration, Store);
                            break;

                        case RavenDatabaseMode.Sharded:
                            await ClusterTestBase.Sharding.Backup.UpdateServerWideConfigAsync(ClusterTestBase.Server, ServerWideBackupConfiguration, Store);
                            break;

                        case RavenDatabaseMode.All:
                        default:
                            throw new ArgumentOutOfRangeException(nameof(DatabaseMode), DatabaseMode, null);
                    }
                    break;
            }
        }

        public async Task<TimeSpan> MeasurePeriodicBackupDurationAsync()
        {
            switch (DatabaseMode)
            {
                case RavenDatabaseMode.Single when BackupKind == BackupKind.Full:
                    return await MeasureNonShardedBackupDurationAsync();

                case RavenDatabaseMode.Single when BackupKind == BackupKind.Incremental:
                    await CreateDocumentsForNonShardedDatabaseAsync();
                    return await MeasureNonShardedBackupDurationAsync();

                case RavenDatabaseMode.Sharded when BackupKind == BackupKind.Full:
                    return await MeasureShardedBackupDurationAsync();

                case RavenDatabaseMode.Sharded when BackupKind == BackupKind.Incremental:
                    await CreateDocumentsOnTheSameShardAsInitialDocumentAsync();
                    return await MeasureShardedBackupDurationAsync();

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public async Task<TimeSpan> MeasureOneTimeBackupDurationAsync()
        {
            var sw = Stopwatch.StartNew();
            var operation = await Store.Maintenance.SendAsync(new BackupOperation(OneTimeBackupConfiguration));
            await operation.WaitForCompletionAsync(DefaultTimeout);
            return sw.Elapsed;
        }

        private async Task<TimeSpan> MeasureNonShardedBackupDurationAsync()
        {
            var sw = Stopwatch.StartNew();
            await ClusterTestBase.Backup.RunBackupAsync(ClusterTestBase.Server, _taskId, Store, isFullBackup: BackupKind == BackupKind.Full);
            return sw.Elapsed;
        }

        private async Task<TimeSpan> MeasureShardedBackupDurationAsync()
        {
            var sw = Stopwatch.StartNew();
            var waitHandles = await ClusterTestBase.Sharding.Backup.WaitForBackupsToComplete([ClusterTestBase.Server], Store.Database);
            await ClusterTestBase.Sharding.Backup.RunBackupAsync(Store, _taskId, isFullBackup: BackupKind == BackupKind.Full);
            WaitHandle.WaitAll(waitHandles, DefaultTimeout);
            return sw.Elapsed;
        }
    }

    private class MaxReadOpsPerSecOptionRestoreTestScenario : MaxReadOpsPerSecOptionTestScenarioBase
    {
        public MaxReadOpsPerSecOptionRestoreTestScenario(ClusterTestBase clusterTestBase, Options options, [CallerMemberName] string testName = null)
            : base(clusterTestBase, options, testName) { }

        protected override int DocumentsToCreate => BackupType == BackupType.Backup ? 150 : 5;
        protected internal override int MaxReadOpsPerSecToTest => BackupType == BackupType.Backup ? 10 : 1;
        protected override int CalculateExpectedMinimumOperationDurationInSeconds()
        {
            switch (BackupType)
            {
                case BackupType.Backup:
                    // Minus `MaxReadOpsPerSecToTest` because rateGate will not wait after the last operation
                    return (DocumentsToCreate - MaxReadOpsPerSecToTest) / MaxReadOpsPerSecToTest;

                case BackupType.Snapshot:
                    // snapshot data requires 11 buffer iterations for db with 5 docs (value obtained experimentally)
                    return 11;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        protected override string OperationName => "Restore";

        private RestoreBackupConfiguration RestoreBackupConfiguration { get; set; }

        public async Task CreateInitialDocumentsAsync()
        {
            switch (DatabaseMode)
            {
                case RavenDatabaseMode.Single:
                    await CreateDocumentsForNonShardedDatabaseAsync();
                    break;
                case RavenDatabaseMode.Sharded:
                    // We want to store all documents in the same shard to get clear understanding the number of documents per shard to do clear measurements
                    await CreateInitialSingleDocumentAsync();
                    await CreateDocumentsOnTheSameShardAsInitialDocumentAsync();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(DatabaseMode), DatabaseMode, null);
            }
        }

        private async Task CreateDocumentsOnTheSameShardAsInitialDocumentAsync()
        {
            for (int i = 1; i < DocumentsToCreate; i++)
                using (var session = Store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"Name{i}" }, $"{nameof(User)}s/{i}${InitialDocumentId}");
                    await session.SaveChangesAsync();
                }
        }

        public async Task PrepareBackupFileToRestore()
        {
            var backupConfig = ClusterTestBase.Backup.CreateBackupConfiguration(BackupPath, BackupType);

            switch (DatabaseMode)
            {
                case RavenDatabaseMode.Single:
                    await ClusterTestBase.Backup.UpdateConfigAndRunBackupAsync(ClusterTestBase.Server, backupConfig, Store);

                    RestoreBackupConfiguration = new RestoreBackupConfiguration
                    {
                        BackupLocation = Directory.GetDirectories(BackupPath).First(),
                        DatabaseName = $"restored_with_defaults_database-{Guid.NewGuid()}"
                    };
                    break;

                case RavenDatabaseMode.Sharded:
                    var waitHandles = await ClusterTestBase.Sharding.Backup.WaitForBackupsToComplete([ClusterTestBase.Server], Store.Database);
                    await ClusterTestBase.Sharding.Backup.UpdateConfigurationAndRunBackupAsync([ClusterTestBase.Server], Store, backupConfig);
                    Assert.True(WaitHandle.WaitAll(waitHandles, DefaultTimeout));

                    var dirs = Directory.GetDirectories(BackupPath);
                    var sharding = await ClusterTestBase.Sharding.GetShardingConfigurationAsync(Store);
                    var settings = ClusterTestBase.Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                    RestoreBackupConfiguration = new RestoreBackupConfiguration
                    {
                        DatabaseName = $"restored_with_defaults_database-{Guid.NewGuid()}",
                        ShardRestoreSettings = settings
                    };
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(DatabaseMode), DatabaseMode, null);
            }
        }

        public TimeSpan MeasureShardedDatabaseRestoreDuration()
        {
            var cw = Stopwatch.StartNew();

            using (ClusterTestBase.Sharding.Backup.ReadOnly(BackupPath))
            using (ClusterTestBase.Backup.RestoreDatabase(Store, RestoreBackupConfiguration));

            return cw.Elapsed;
        }

        public void SetMaxReadOpsPerSecond()
        {
            RestoreBackupConfiguration.MaxReadOpsPerSecond = MaxReadOpsPerSecToTest;
            RestoreBackupConfiguration.DatabaseName = $"restored_with_maxReadOps_database-{Guid.NewGuid()}";
        }
    }
}
