using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Converters;
using Raven.Client.Server.Operations;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Sdk;
using ModifyDatabaseWatchers = Raven.Client.Server.Operations.ModifyDatabaseWatchers;

namespace FastTests.Server.Replication
{
    public class ReplicationTestsBase : ClusterTestBase
    {
        protected List<object> GetRevisions(DocumentStore store, string id)
        {
            using (var commands = store.Commands())
            {
                var command = new GetRevisionsCommand(id);

                commands.RequestExecutor.Execute(command, commands.Context);

                return command.Result;
            }
        }

        protected void EnsureReplicating(DocumentStore src, DocumentStore dst)
        {
            var id = "marker/" + Guid.NewGuid().ToString();
            using (var s = src.OpenSession())
            {
                s.Store(new { }, id);
                s.SaveChanges();
            }
            WaitForDocumentToReplicate<object>(dst, id, 15 * 1000);
        }

        protected Dictionary<string, string[]> GetConnectionFaliures(DocumentStore store)
        {
            using (var commands = store.Commands())
            {
                var command = new GetConncectionFailuresCommand();

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
                                             $"on database {store.DefaultDatabase}");
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

        protected bool WaitForDocument<T>(IDocumentStore store,
            string docId,
            Func<T, bool> predicate,
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
                        var doc = session.Load<T>(docId);
                        if (doc != null && predicate(doc))
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
                var doc = session.Load<T>(docId);
                if (doc != null && predicate(doc))
                    return true;
            }
            return false;
        }

        protected bool WaitForDocument(DocumentStore store,
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
                        if (doc != null)
                            return true;
                    }
                    catch (DocumentConflictException)
                    {
                        // expected that we might get conflict, ignore and wait
                    }
                }
            }
            using (var session = store.OpenSession())
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<dynamic>(docId);
                if (doc != null)
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
                using (var session = store.OpenSession(store.DefaultDatabase))
                {
                    var doc = session.Load<T>(id);
                    if (doc != null)
                        return doc;
                }
            }

            return default(T);
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


        protected static async Task<ModifyDatabaseWatchersResult> UpdateReplicationTopology(
            DocumentStore store,
            List<DatabaseWatcher> watchers)
        {
            var cmd = new ModifyDatabaseWatchers(store.DefaultDatabase, watchers);
            return await store.Admin.Server.SendAsync(cmd);
        }

        protected static async Task<ModifySolverResult> UpdateConflictResolver(IDocumentStore store, string resovlerDbId = null, Dictionary<string, ScriptResolver> collectionByScript = null, bool resolveToLatest = false)
        {
            var cmd = new ModifyConflictSolverOperation(store.DefaultDatabase,
                resovlerDbId, collectionByScript, resolveToLatest);
            return await store.Admin.Server.SendAsync(cmd);
        }

        public DatabaseTopology CurrentDatabaseTopology;

        public async Task SetupReplicationAsync(DocumentStore fromStore, params DocumentStore[] toStores)
        {
            var watchers = new List<DatabaseWatcher>();
            foreach (var store in toStores)
            {
                var databaseWatcher = new DatabaseWatcher
                {
                    Database = store.DefaultDatabase,
                    Url = store.Url,
                };
                ModifyReplicationDestination(databaseWatcher);
                watchers.Add(databaseWatcher);
            }
            var result = await UpdateReplicationTopology(fromStore, watchers);
            CurrentDatabaseTopology = result.Topology;
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
            var watchers = new List<DatabaseWatcher>();
            foreach (var node in toNodes)
            {
                watchers.Add(new DatabaseWatcher
                {
                    Database = node.Database,
                    Url = node.Url,
                });
            }
            await UpdateReplicationTopology(fromStore, watchers);
        }

        private class GetRevisionsCommand : RavenCommand<List<object>>
        {
            private readonly string _id;

            public GetRevisionsCommand(string id)
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                _id = id;
            }
            public override bool IsReadRequest => true;
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/revisions?key={_id}";

                ResponseType = RavenCommandResponseType.Object;
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

                var result = new List<object>();
                foreach (var arrayItem in array.Items)
                {
                    result.Add(arrayItem);
                }

                Result = result;
            }
        }

        private class GetConncectionFailuresCommand : RavenCommand<Dictionary<string, string[]>>
        {

            public override bool IsReadRequest => true;
            public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/debug/incoming-rejection-info";

                ResponseType = RavenCommandResponseType.Array;
                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }
            public override void SetResponse(BlittableJsonReaderArray response, bool fromCache)
            {
                List<string> list = new List<string>();
                Dictionary<string, string[]> result = new Dictionary<string, string[]>();
                foreach (BlittableJsonReaderObject responseItem in response.Items)
                {
                    BlittableJsonReaderObject obj;
                    responseItem.TryGet("Key", out obj);
                    string name;
                    obj.TryGet("SourceDatabaseName", out name);

                    BlittableJsonReaderArray arr;
                    responseItem.TryGet("Value", out arr);
                    list.Clear();
                    foreach (BlittableJsonReaderObject arrItem in arr)
                    {
                        string reason;
                        arrItem.TryGet("Reason", out reason);
                        list.Add(reason);
                    }
                    result.Add(name, list.ToArray());
                }
                Result = result;
            }
            public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
            {
                throw new NotImplementedException();
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
                    string key;
                    if (json.TryGet("Key", out key) == false)
                        ThrowInvalidResponse();

                    result.Add(key);
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