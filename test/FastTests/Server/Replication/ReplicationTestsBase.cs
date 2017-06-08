using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Sdk;

namespace FastTests.Server.Replication
{
    public class ReplicationTestsBase : ClusterTestBase
    {
        protected void EnsureReplicating(DocumentStore src, DocumentStore dst)
        {
            var id = "marker/" + Guid.NewGuid();
            using (var s = src.OpenSession())
            {
                s.Store(new { }, id);
                s.SaveChanges();
            }
            WaitForDocumentToReplicate<object>(dst, id, 15 * 1000);
        }

        protected Dictionary<string, string[]> GetConnectionFailures(DocumentStore store)
        {
            using (var commands = store.Commands())
            {
                var command = new GetConnectionFailuresCommand();

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected GetConflictsResult GetConflicts(IDocumentStore store, string docId)
        {
            using (var commands = store.Commands())
            {
                var command = new GetReplicationConflictsCommand(docId);

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected GetConflictsResult WaitUntilHasConflict(IDocumentStore store, string docId, int count = 2)
        {
            int timeout = 5000;

            if (Debugger.IsAttached)
                timeout *= 100;
            GetConflictsResult conflicts;
            var sw = Stopwatch.StartNew();
            do
            {
                conflicts = GetConflicts(store, docId);

                if (conflicts.Results.Length >= count)
                    break;

                if (sw.ElapsedMilliseconds > timeout)
                {
                    throw new XunitException($"Timed out while waiting for conflicts on {docId} we have {conflicts.Results.Length} conflicts " +
                                             $"on database {store.Database}");
                }
            } while (true);

            return conflicts;
        }

        protected bool WaitForDocumentDeletion(DocumentStore store,
            string docId,
            int timeout = 10000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession())
                {
                    try
                    {
                        var doc = session.Load<dynamic>(docId);
                        if (doc == null)
                            return true;
                    }
                    catch (ConflictException)
                    {
                        // expected that we might get conflict, ignore and wait
                    }
                }
            }
            using (var session = store.OpenSession())
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<dynamic>(docId);
                if (doc == null)
                    return true;
            }
            return false;
        }
        
        protected List<string> WaitUntilHasTombstones(
                IDocumentStore store,
                int count = 1)
        {

            int timeout = 5000;
            if (Debugger.IsAttached)
                timeout *= 100;
            List<string> tombstones;
            var sw = Stopwatch.StartNew();
            do
            {
                tombstones = GetTombstones(store);

                if (tombstones == null ||
                    tombstones.Count >= count)
                    break;

                if (sw.ElapsedMilliseconds > timeout)
                {
                    Assert.False(true, store.Identifier + " -> Timed out while waiting for tombstones, we have " + tombstones.Count + " tombstones, but should have " + count);
                }

            } while (true);
            return tombstones ?? new List<string>();
        }


        protected List<string> GetTombstones(IDocumentStore store)
        {
            using (var commands = store.Commands())
            {
                var command = new GetReplicationTombstonesCommand();

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected LiveTopologyInfo GetLiveTopology(IDocumentStore store)
        {
            using (var commands = store.Commands())
            {
                var command = new GetLiveTopologyCommand();

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected T WaitForDocumentToReplicate<T>(IDocumentStore store, string id, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenSession(store.Database))
                {
                    var doc = session.Load<T>(id);
                    if (doc != null)
                        return doc;
                }
            }

            return null;
        }

        public class SetupResult : IDisposable
        {
            public IReadOnlyList<ServerNode> Nodes;
            public IDocumentStore LeaderStore;
            public string Database;

            public void Dispose()
            {
                LeaderStore?.Dispose();
            }
        }

        protected static async Task<ModifyOngoingTaskResult> AddWatcherToReplicationTopology(
            DocumentStore store,
            DatabaseWatcher watcher)
        {
            var op = new UpdateExternalReplicationOperation(store.Database, watcher);
            return await store.Admin.Server.SendAsync(op);
        }

        protected static async Task<ModifyOngoingTaskResult> DeleteOngoingTask(
            DocumentStore store,
            long taskId,
            OngoingTaskType taskType)
        {
            var op = new DeleteOngoingTaskOperation(store.Database, taskId, taskType);
            return await store.Admin.Server.SendAsync(op);
        }

        protected static async Task<GetTaskInfoResult> GetTaskInfo(
            DocumentStore store,
            long taskId, OngoingTaskType type)
        {
            var op = new GetOngoingTaskInfoOperation(store.Database, taskId, type);
            return await store.Admin.Server.SendAsync(op);
        }

        protected static async Task<ModifySolverResult> UpdateConflictResolver(IDocumentStore store, string resovlerDbId = null, Dictionary<string, ScriptResolver> collectionByScript = null, bool resolveToLatest = false)
        {
            var op = new ModifyConflictSolverOperation(store.Database,
                resovlerDbId, collectionByScript, resolveToLatest);
            return await store.Admin.Server.SendAsync(op);
        }

        public async Task SetupReplicationAsync(DocumentStore fromStore, params DocumentStore[] toStores)
        {
            foreach (var store in toStores)
            {
                var databaseWatcher = new DatabaseWatcher
                {
                    Database = store.Database,
                    Url = store.Urls[0]
                };
                ModifyReplicationDestination(databaseWatcher);
                await AddWatcherToReplicationTopology(fromStore, databaseWatcher);
            }
        }

        public static async Task SetScriptResolutionAsync(DocumentStore store, string script, string collection)
        {
            var resolveByCollection = new Dictionary<string, ScriptResolver>
            {
                {
                    collection, new ScriptResolver
                    {
                        Script = script
                    }
                }
            };
            await UpdateConflictResolver(store, null, resolveByCollection);
        }

        protected async Task SetupReplicationAsync(DocumentStore fromStore, ConflictSolver conflictSolver, params DocumentStore[] toStores)
        {
            await UpdateConflictResolver(fromStore, conflictSolver.DatabaseResolverId, conflictSolver.ResolveByCollection, conflictSolver.ResolveToLatest);
            await SetupReplicationAsync(fromStore, toStores);
        }

        protected static async Task SetReplicationConflictResolutionAsync(DocumentStore store, StraightforwardConflictResolution conflictResolution)
        {
            await UpdateConflictResolver(store, null, null, conflictResolution == StraightforwardConflictResolution.ResolveToLatest);
        }

        protected virtual void ModifyReplicationDestination(ReplicationNode replicationNode)
        {
        }

        protected static async Task SetupReplicationWithCustomDestinations(DocumentStore fromStore, params ReplicationNode[] toNodes)
        {
            foreach (var node in toNodes)
            {
                var databaseWatcher = new DatabaseWatcher
                {
                    Database = node.Database,
                    Url = node.Url,
                };
                await AddWatcherToReplicationTopology(fromStore, databaseWatcher);
            }
        }

        private class GetConnectionFailuresCommand : RavenCommand<Dictionary<string, string[]>>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/debug/incoming-rejection-info";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null ||
                    response.TryGet("Stats", out BlittableJsonReaderArray stats) == false)
                {
                    ThrowInvalidResponse();
                    return; // never hit
                }

                var list = new List<string>();
                var result = new Dictionary<string, string[]>();
                foreach (BlittableJsonReaderObject stat in stats)
                {
                    stat.TryGet("Key", out BlittableJsonReaderObject obj);
                    obj.TryGet("SourceDatabaseName", out string name);
                    stat.TryGet("Value", out BlittableJsonReaderArray arr);
                    list.Clear();
                    foreach (BlittableJsonReaderObject arrItem in arr)
                    {
                        arrItem.TryGet("Reason", out string reason);
                        list.Add(reason);
                    }
                    result.Add(name, list.ToArray());
                }
                Result = result;
            }
        }

        private class GetLiveTopologyCommand : RavenCommand<LiveTopologyInfo>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/debug/live-topology";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.LiveTopologyInfo(response);
            }
        }

        private class GetReplicationTombstonesCommand : RavenCommand<List<string>>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/tombstones";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                BlittableJsonReaderArray array;
                if (response.TryGet("Results", out array) == false)
                    ThrowInvalidResponse();

                var result = new List<string>();
                foreach (BlittableJsonReaderObject json in array)
                {
                    if (json.TryGet("Id", out string id) == false)
                        ThrowInvalidResponse();

                    result.Add(id);
                }

                Result = result;
            }
        }

        private class GetReplicationConflictsCommand : RavenCommand<GetConflictsResult>
        {
            private readonly string _id;

            public GetReplicationConflictsCommand(string id)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                _id = id;
            }

            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/conflicts?docId={_id}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                Result = JsonDeserializationClient.GetConflictsResult(response);
            }
        }
    }
}