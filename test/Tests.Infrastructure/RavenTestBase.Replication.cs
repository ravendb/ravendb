using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions;
using Raven.Server;
using Raven.Server.Documents;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly ReplicationTestBase2 Replication;

    public class ReplicationTestBase2
    {
        private readonly RavenTestBase _parent;

        public ReplicationTestBase2(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public async Task WaitForConflict(IDocumentStore slave, string id, int timeout = 15_000)
        {
            var timeoutAsTimeSpan = TimeSpan.FromMilliseconds(timeout);
            var sw = Stopwatch.StartNew();

            while (sw.Elapsed < timeoutAsTimeSpan)
            {
                using (var session = slave.OpenAsyncSession())
                {
                    try
                    {
                        await session.LoadAsync<dynamic>(id);
                        await Task.Delay(100);
                    }
                    catch (ConflictException)
                    {
                        return;
                    }
                }
            }

            throw new InvalidOperationException($"Waited '{sw.Elapsed}' for conflict on '{id}' but it did not happen.");
        }

        public bool WaitForCounterReplication(IEnumerable<IDocumentStore> stores, string docId, string counterName, long expected, TimeSpan timeout)
        {
            long? val = null;
            var sw = Stopwatch.StartNew();

            foreach (var store in stores)
            {
                val = null;
                while (sw.Elapsed < timeout)
                {
                    val = store.Operations
                        .Send(new GetCountersOperation(docId, new[] { counterName }))
                        .Counters[0]?.TotalValue;

                    if (val == expected)
                        break;

                    Thread.Sleep(100);
                }
            }

            return val == expected;
        }

        public async Task<T> WaitForDocumentToReplicateAsync<T>(IDocumentStore store, string id, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenAsyncSession(store.Database))
                {
                    var doc = await session.LoadAsync<T>(id);
                    if (doc != null)
                        return doc;
                }

                await Task.Delay(100);
            }

            return null;
        }

        public Task<string> GetErrorsAsync(IDocumentStore store) => GetErrorsForClusterAsync(new List<RavenServer> { _parent.Server }, store.Database);

        public async Task<string> GetErrorsForClusterAsync(IEnumerable<RavenServer> servers, string database)
        {
            var sb = new StringBuilder();
            foreach (var server in servers)
            {
                var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
                sb.AppendLine($"Replication info for {server.ServerStore.NodeTag}");
                sb.AppendLine(" --- Outgoing ---");
                sb.AppendLine(string.Join($"{Environment.NewLine}", GetOutgoingStatus(db)));
                sb.AppendLine();
                sb.AppendLine(" --- Incoming ---");
                sb.AppendLine(string.Join($"{Environment.NewLine}", GetIncomingStatus(db)));
                sb.AppendLine();
            }

            return sb.ToString();
        }

        private IEnumerable<string> GetOutgoingStatus(DocumentDatabase database)
        {
            foreach (var item in database.ReplicationLoader.OutgoingFailureInfo)
            {
                var key = item.Key;
                if (item.Value.Errors.Count == 0)
                {
                    yield return $"{key}: No errors";
                    continue;
                }

                var errors = string.Join(Environment.NewLine, item.Value.Errors);
                yield return $"{key} has following errors:{Environment.NewLine}{errors}";
            }
        }

        private IEnumerable<string> GetIncomingStatus(DocumentDatabase database)
        {
            foreach (var item in database.ReplicationLoader.IncomingRejectionStats)
            {
                var key = item.Key;

                if (item.Value.Count == 0)
                {
                    yield return $"{key.DebugInfoString()}: No errors";
                    continue;
                }

                var errors = string.Join(Environment.NewLine, item.Value.Select(v => $"[{v.When:O}] {v.Reason}"));
                yield return $"{key.DebugInfoString()} has following errors:{Environment.NewLine}{errors}";
            }

            foreach (var incoming in database.ReplicationLoader.IncomingConnections)
            {
                yield return $"{incoming.DebugInfoString()}: No errors";
            }
        }
    }
}
