using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Documents;
using Xunit;

namespace Tests.Infrastructure.Utils
{
    internal static class DebugHelper
    {
        public static async Task<IDisposable> GatherVerboseDatabaseDisposeInformationAsync(RavenServer server, int timeoutInMs)
        {
            if (server?._forTestingPurposes == null || server._forTestingPurposes.GatherVerboseDatabaseDisposeInformation == false)
                return null;

            var databaseDisposeLog = new ConcurrentDictionary<string, ConcurrentQueue<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var databaseTask in server.ServerStore.DatabasesLandlord.DatabasesCache.Values)
            {
                if (databaseTask.IsFaulted)
                    continue;

                Assert.True(await databaseTask.WaitWithTimeout(TimeSpan.FromMilliseconds(timeoutInMs)));
                var database = await databaseTask;
                AttachDatabaseDisposeLog(databaseDisposeLog, database);
            }

            return new DisposableAction(() =>
            {
                PrintDatabaseDisposeLog(databaseDisposeLog);
            });
        }

        public static IDisposable GatherVerboseDatabaseDisposeInformation(RavenServer server, int timeoutInMs)
        {
            if (server?._forTestingPurposes == null || server._forTestingPurposes.GatherVerboseDatabaseDisposeInformation == false)
                return null;

            var databaseDisposeLog = new ConcurrentDictionary<string, ConcurrentQueue<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var databaseTask in server.ServerStore.DatabasesLandlord.DatabasesCache.Values)
            {
                if (databaseTask.IsFaulted)
                    continue;

                Assert.True(databaseTask.Wait(TimeSpan.FromMilliseconds(timeoutInMs)));
                var database = databaseTask.Result;
                AttachDatabaseDisposeLog(databaseDisposeLog, database);
            }

            return new DisposableAction(() =>
            {
                PrintDatabaseDisposeLog(databaseDisposeLog);
            });
        }

        private static void AttachDatabaseDisposeLog(ConcurrentDictionary<string, ConcurrentQueue<string>> log, DocumentDatabase database)
        {
            database.ForTestingPurposesOnly().DisposeLog += (name, message) =>
            {
                var logs = log.GetOrAdd(name, _ => new ConcurrentQueue<string>());
                logs.Enqueue($"[{DateTime.UtcNow:O}] {message}");
            };
        }

        private static void PrintDatabaseDisposeLog(ConcurrentDictionary<string, ConcurrentQueue<string>> log)
        {
            if (log == null)
                return;

            var sb = new StringBuilder($"Databases Dispose Log:{Environment.NewLine}");
            foreach (var kvp in log)
            {
                foreach (string message in kvp.Value)
                    sb.AppendLine($"[{kvp.Key}] {message}");
            }

            Console.WriteLine(sb);
        }
    }
}
