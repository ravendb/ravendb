using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Sdk;

namespace FastTests.Server.Replication
{
    public class ReplicationTestBase : ClusterTestBase
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

        protected GetConflictsResult.Conflict[] WaitUntilHasConflict(IDocumentStore store, string docId, int count = 2)
        {
            var timeout = 5000;
            if (Debugger.IsAttached)
                timeout *= 100;
            var sw = Stopwatch.StartNew();
            do
            {
                var conflicts = store.Commands().GetConflictsFor(docId);
                if (conflicts.Length >= count)
                    return conflicts;

                if (sw.ElapsedMilliseconds > timeout)
                {
                    throw new XunitException($"Timed out while waiting for conflicts on {docId} we have {conflicts.Length} conflicts " +
                                             $"on database {store.Database}");
                }
                Thread.Sleep(100);
            } while (true);
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
                Thread.Sleep(100);
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
                Thread.Sleep(100);
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
                Thread.Sleep(100);
            }

            return null;
        }

        protected T WaitForDocumentWithAttachmentToReplicate<T>(IDocumentStore store, string id, string attachmentName, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenSession(store.Database))
                {
                    var doc = session.Load<T>(id);
                    if (doc != null && session.Advanced.Attachments.Exists(id, attachmentName))                   
                        return doc;                    
                }
                Thread.Sleep(100);
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

        protected static async Task<ModifyOngoingTaskResult> AddWatcherToReplicationTopology<T>(
            DocumentStore store,
            T watcher,
            string[] urls = null) where T : ExternalReplication
        {
            await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = watcher.ConnectionStringName,
                Database = watcher.Database,
                TopologyDiscoveryUrls = urls ?? store.Urls
            }));

            IMaintenanceOperation<ModifyOngoingTaskResult> op;
            if (watcher is PullReplicationAsSink pull)
            {
                op = new UpdatePullReplicationAsSinkOperation(pull);
            }
            else
            {
                op = new UpdateExternalReplicationOperation(watcher);
            }
            return await store.Maintenance.SendAsync(op);
        }

        protected static async Task<ModifyOngoingTaskResult> DeleteOngoingTask(
            DocumentStore store,
            long taskId,
            OngoingTaskType taskType)
        {
            var op = new DeleteOngoingTaskOperation(taskId, taskType);
            return await store.Maintenance.SendAsync(op);
        }

        protected static async Task<OngoingTask> GetTaskInfo(
            DocumentStore store,
            long taskId, OngoingTaskType type)
        {
            var op = new GetOngoingTaskInfoOperation(taskId, type);
            return await store.Maintenance.SendAsync(op);
        }

        protected static async Task<ModifySolverResult> UpdateConflictResolver(IDocumentStore store, Dictionary<string, ScriptResolver> collectionByScript = null, bool resolveToLatest = false)
        {
            var op = new ModifyConflictSolverOperation(store.Database, collectionByScript, resolveToLatest);
            return await store.Maintenance.Server.SendAsync(op);
        }

        public async Task<List<ModifyOngoingTaskResult>> SetupReplicationAsync(DocumentStore fromStore, params DocumentStore[] toStores)
        {
            var tasks = new List<Task<ModifyOngoingTaskResult>>();
            var resList = new List<ModifyOngoingTaskResult>();
            foreach (var store in toStores)
            {
                var databaseWatcher = new ExternalReplication(store.Database, $"ConnectionString-{store.Identifier}");
                ModifyReplicationDestination(databaseWatcher);
                tasks.Add(AddWatcherToReplicationTopology(fromStore, databaseWatcher, store.Urls));
            }
            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                resList.Add(await task);
            }
            return resList;
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
            await UpdateConflictResolver(store, resolveByCollection);
        }

        protected async Task SetupReplicationAsync(DocumentStore fromStore, ConflictSolver conflictSolver, params DocumentStore[] toStores)
        {
            await UpdateConflictResolver(fromStore, conflictSolver.ResolveByCollection, conflictSolver.ResolveToLatest);
            await SetupReplicationAsync(fromStore, toStores);
        }

        protected static async Task SetReplicationConflictResolutionAsync(DocumentStore store, StraightforwardConflictResolution conflictResolution)
        {
            await UpdateConflictResolver(store, null, conflictResolution == StraightforwardConflictResolution.ResolveToLatest);
        }

        protected virtual void ModifyReplicationDestination(ReplicationNode replicationNode)
        {
        }

        protected static async Task SetupReplicationWithCustomDestinations(DocumentStore fromStore, params ReplicationNode[] toNodes)
        {
            foreach (var node in toNodes)
            {
                var databaseWatcher = new ExternalReplication(node.Database, "connectin");
                await AddWatcherToReplicationTopology(fromStore, databaseWatcher, new[] { node.Url });
            }
        }

        protected static async Task<OngoingTasksHandler> InstantiateOutgoingTaskHandler(string name, RavenServer server)
        {
            Assert.True(server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(name, out var db));
            var database = await db;
            var handler = new OngoingTasksHandler();
            var ctx = new RequestHandlerContext
            {
                RavenServer = server,
                Database = database,
                HttpContext = new DefaultHttpContext()
            };
            handler.Init(ctx);
            return handler;
        }

        protected async Task<(DocumentStore source, DocumentStore destination)> CreateDuoCluster([CallerMemberName] string caller = null)
        {
            var leader = await CreateRaftClusterAndGetLeader(2);
            var follower = Servers.First(srv => ReferenceEquals(srv, leader) == false);
            var source = new DocumentStore
            {
                Urls = new[] { leader.WebUrl },
                Database = caller
            };
            var destination = new DocumentStore
            {
                Urls = new[] { follower.WebUrl },
                Database = caller
            };
            source.Initialize();
            destination.Initialize();

            var res = CreateClusterDatabase(caller, source, 2);
            //var doc = MultiDatabase.CreateDatabaseDocument(dbName);
            //var databaseResult = source.Admin.Server.Send(new CreateDatabaseOperation(doc, 2));
            await WaitForRaftIndexToBeAppliedInCluster(res.RaftCommandIndex, TimeSpan.FromSeconds(5));
            return (source, destination);
        }


        private class GetConnectionFailuresCommand : RavenCommand<Dictionary<string, string[]>>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/debug/incoming-rejection-info";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
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

        private class GetReplicationTombstonesCommand : RavenCommand<List<string>>
        {
            public override bool IsReadRequest => true;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/replication/tombstones";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
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
    }
}
