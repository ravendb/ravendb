using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Features;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Platform.Posix;

namespace RequestHandler.Benchmark;

public class RavenDbInstance : IDisposable
{
    public const string DatabaseName = "Benchmark";
    public IDocumentStore Store;
    public RavenServer Server;
    public DocumentDatabase Database;
    private string _pathToServer;

    static RavenDbInstance()
    {
        LicenseManager.IgnoreProcessorAffinityChanges = true;
        LicenseManager.IgnoreCompressionLicenseLimit = true;
        BackupUtils.IgnoreHealthChecksBeforeBackup = true;

        //RequestExecutor.HttpClientFactory = RavenServerHttpClientFactory.Instance;
        LicenseManager.AddLicenseStatusToLicenseLimitsException = true;
        RachisStateMachine.EnableDebugLongCommit = true;
        RavenServer.SkipCertificateDispose = true;

        NativeMemory.GetCurrentUnmanagedThreadId = () => (ulong)Pal.rvn_get_current_thread_id();
        ZstdLib.CreateDictionaryException = message => new VoronErrorException(message);
    }

    private static void DeletePath(string pathToDelete, ExceptionAggregator exceptionAggregator)
    {
        FileAttributes pathAttributes;
        try
        {
            pathAttributes = File.GetAttributes(pathToDelete);
        }
        catch (FileNotFoundException)
        {
            return;
        }
        catch (DirectoryNotFoundException)
        {
            return;
        }

        if (pathAttributes.HasFlag(FileAttributes.Directory))
            exceptionAggregator.Execute(() => ClearDatabaseDirectory(pathToDelete));
        else
            exceptionAggregator.Execute(() => IOExtensions.DeleteFile(pathToDelete));
    }


    private static void ClearDatabaseDirectory(string dataDir)
    {
        var isRetry = false;

        while (true)
        {
            try
            {
                IOExtensions.DeleteDirectory(dataDir);
                break;
            }
            catch (IOException)
            {
                if (isRetry)
                    throw;

                GC.Collect();
                GC.WaitForPendingFinalizers();
                isRetry = true;

                Thread.Sleep(200);
            }
        }
    }

    private static async Task<DocumentDatabase> GetDatabase(RavenServer ravenServer, string databaseName)
    {
        var database = await ravenServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);
        if (database != null) return database;
        
        // Throw and get more info why database is null
        using (ravenServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            context.OpenReadTransaction();
            var lastCommit = ravenServer.ServerStore.Engine.GetLastCommitIndex(context);
            var doc = ravenServer.ServerStore.Cluster.Read(context, "db/" + databaseName.ToLowerInvariant());
            throw new InvalidOperationException("For " + databaseName + ". Database is null and database record is: " + (doc == null ? "null" : doc.ToString()) +
                                                " Last commit: " + lastCommit);
        }
    }

    private static string NewDataPath(string? testName, int serverPort, bool forceCreateDir = false)
    {
        testName = testName?.Replace("<", "").Replace(">", "");

        var newDataDir = Path.GetFullPath($".\\Databases\\{testName ?? "TestDatabase"}.{serverPort}-{0}");

        if (PlatformDetails.RunningOnPosix)
            newDataDir = PosixHelper.FixLinuxPath(newDataDir);

        if (forceCreateDir && Directory.Exists(newDataDir) == false)
            Directory.CreateDirectory(newDataDir);

        return newDataDir;
    }
    
    public void InitializeDatabase()
    {
        var configuration = RavenConfiguration.CreateForServer(Guid.NewGuid().ToString());
        configuration.Initialize();

        configuration.Server.Name = "RequestsBenchmark";
        configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(60, TimeUnit.Seconds);
        configuration.Licensing.EulaAccepted = true;
        configuration.Logs.Mode = LogMode.None;
        configuration.Core.RunInMemory = false;
        configuration.Core.FeaturesAvailability = FeaturesAvailability.Experimental;
        configuration.Core.ServerUrls = ["http://127.0.0.1:0"];
        _pathToServer = NewDataPath("RequestsBenchmark", 0, true);
        configuration.Core.DataDirectory = new PathSetting(_pathToServer);
        Server = new RavenServer(configuration)
        {
            ThrowOnLicenseActivationFailure = true,
            DebugTag = "A"
        };

        Server.Initialize();
        Server.ServerStore.ValidateFixedPort = false;
        AsyncHelpers.RunSync(() => Server.ServerStore.EnsureNotPassiveAsync());
        //var dbPath = new PathSetting(Configuration.PathToDatabase);

        var doc = new DatabaseRecord(DatabaseName)
        {
            Settings =
            {
                //[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = dbPath.ToFullPath(),
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = false.ToString(),
                [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true",
                [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString(),
                [RavenConfiguration.GetKey(x => x.Queries.RegexTimeout)] = (250).ToString(),
                //[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = Configuration.PathToDatabase,
            }
        };

        Store = new DocumentStore
        {
            Urls = [Server.WebUrl],
            Database = DatabaseName,
        };
        Store.Initialize();

        Store.Maintenance.Server.Send(new DeleteDatabasesOperation(DatabaseName, hardDelete: false));
        Store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

        Database = AsyncHelpers.RunSync(() => GetDatabase(Server, DatabaseName));
    }

    public void Dispose()
    {
        var exceptions = new ExceptionAggregator("Found exceptions during dispose");
        exceptions.Execute(Store);
        exceptions.Execute(Server);
        DeletePath(_pathToServer, exceptions);
        exceptions.ThrowIfNeeded();
    }
}
