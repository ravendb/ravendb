using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server;
using Raven.Server.Commercial;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Platform.Posix;

namespace Requests.Benchmark;

// Due to problematic linking Test projects for benchmarking, this class extracted helpers required to create RavenDB instance.
public partial class RavenDBInstance
{
    static RavenDBInstance()
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
    
    protected static async Task<DocumentDatabase> GetDatabase(RavenServer ravenServer, string databaseName)
    {
        var database = await ravenServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName).ConfigureAwait(false);
        if (database == null)
        {
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

        return database;
    }
    
    public static string NewDataPath(string testName, int serverPort, bool forceCreateDir = false)
    {
        testName = testName?.Replace("<", "").Replace(">", "");

        var newDataDir = Path.GetFullPath($".\\Databases\\{testName ?? "TestDatabase"}.{serverPort}-{0}");

        if (PlatformDetails.RunningOnPosix)
            newDataDir = PosixHelper.FixLinuxPath(newDataDir);

        if (forceCreateDir && Directory.Exists(newDataDir) == false)
            Directory.CreateDirectory(newDataDir);

        return newDataDir;
    }
}
