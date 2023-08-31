using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
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
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Raven.Server.Web.System;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Tests.Infrastructure
{
    public class ReplicationTestBase : ClusterTestBase
    {
        public ReplicationTestBase(ITestOutputHelper output) : base(output)
        {
        }

        public async Task EnsureNoReplicationLoopAsync(IDocumentStore store, RavenDatabaseMode mode)
        {
            using (var manager = await GetReplicationManagerAsync(store, store.Database, mode, new ReplicationManager.ReplicationOptions
                   {
                       BreakReplicationOnStart = false,
                       MaxItemsCount = null
                   }))
            {
                await manager.EnsureNoReplicationLoopAsync();
            }
        }

        public ValueTask<IReplicationManager> GetReplicationManagerAsync(IDocumentStore store, string databaseName, RavenDatabaseMode mode, bool breakReplication = false, List<RavenServer> servers = null)
        {
            var options = new ReplicationManager.ReplicationOptions
            {
                BreakReplicationOnStart = breakReplication
            };

            return GetReplicationManagerAsync(store, databaseName, mode, options, servers);
        }

        public async ValueTask<IReplicationManager> GetReplicationManagerAsync(IDocumentStore store, string databaseName, RavenDatabaseMode mode, ReplicationManager.ReplicationOptions options, List<RavenServer> servers = null)
        {
            if (mode == RavenDatabaseMode.Single)
                return await  ReplicationManager.GetReplicationManagerAsync(servers ?? GetServers() , databaseName, options);

            return await ShardedReplicationTestBase.ShardedReplicationManager.GetShardedReplicationManager(await Sharding.GetShardingConfigurationAsync(store),
                servers ?? GetServers(), databaseName, options);
        }

        public async Task<ReplicationInstance> BreakReplication(Raven.Server.ServerWide.ServerStore from, string databaseName)
        {
            var replication = await ReplicationInstance.GetReplicationInstanceAsync(from.Server, databaseName, new ReplicationManager.ReplicationOptions());
            replication.Break();
            return replication;
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
                {
                    // we need a stable order for testing purposes, should be good enough 
                    return conflicts.OrderBy(x => x.LastModified).ToArray();
                }

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
            int timeout = 10000,
            string database = null)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession(database ?? store.Database))
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
            using (var session = store.OpenSession(database ?? store.Database))
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

            int timeout = 15000;
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

        protected T WaitForDocumentWithSpecificAttachmentInfoToReplicate<T>(IDocumentStore store, string id, string attachmentName, Func<AttachmentDetails, bool> checkAttachment, int timeout)
            where T : class
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds <= timeout)
            {
                using (var session = store.OpenSession(store.Database))
                {
                    var doc = session.Load<T>(id);
                    if (doc != null)
                    {
                        var attachment = session.Advanced.Attachments.Get(id, attachmentName);
                        if (attachment != null && checkAttachment(attachment.Details))
                        {
                            return doc;
                        }
                    }
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

        internal static async Task<ModifyOngoingTaskResult> AddWatcherToReplicationTopology<T>(
            IDocumentStore store,
            T watcher,
            string[] urls = null) where T : ExternalReplicationBase
        {
            var result = await store.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
            {
                Name = watcher.ConnectionStringName,
                Database = watcher.Database,
                TopologyDiscoveryUrls = urls ?? store.Urls
            }));
            Assert.NotNull(result.RaftCommandIndex);

            IMaintenanceOperation<ModifyOngoingTaskResult> op;
            if (watcher is PullReplicationAsSink pull)
            {
                op = new UpdatePullReplicationAsSinkOperation(pull);
            }
            else if (watcher is ExternalReplication ex)
            {
                op = new UpdateExternalReplicationOperation(ex);
            }
            else
            {
                throw new ArgumentException($"Unrecognized type: {watcher.GetType().FullName}");
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

        protected static async Task<OngoingTask> GetTaskInfo(
            DocumentStore store,
            string taskName, OngoingTaskType type)
        {
            var op = new GetOngoingTaskInfoOperation(taskName, type);
            return await store.Maintenance.SendAsync(op);
        }

        protected static async Task<ModifySolverResult> UpdateConflictResolver(IDocumentStore store, Dictionary<string, ScriptResolver> collectionByScript = null, bool resolveToLatest = false)
        {
            var op = new ModifyConflictSolverOperation(store.Database, collectionByScript, resolveToLatest);
            return await store.Maintenance.Server.SendAsync(op);
        }

        public async ValueTask<IReplicationManager> SetupReplicationAndGetManagerAsync(IDocumentStore fromStore, RavenDatabaseMode mode, bool breakReplication = false, List<RavenServer> servers = null, params IDocumentStore[] toStores)
        {
            var replication = await GetReplicationManagerAsync(fromStore, fromStore.Database, mode, breakReplication, servers);
            await SetupReplicationAsync(fromStore, responsibleNode: null, toStores);
            return replication;
        }

        public Task<List<ModifyOngoingTaskResult>> SetupReplicationAsync(IDocumentStore fromStore, params IDocumentStore[] toStores)
        {
            return SetupReplicationAsync(fromStore, responsibleNode: null, toStores);
        }

        public async Task<List<ModifyOngoingTaskResult>> SetupReplicationAsync(IDocumentStore fromStore, string responsibleNode = null, params IDocumentStore[] toStores)
        {
            var tasks = new List<Task<ModifyOngoingTaskResult>>();
            var resList = new List<ModifyOngoingTaskResult>();
            foreach (var store in toStores)
            {
                var databaseWatcher = new ExternalReplication(store.Database, $"ConnectionString-{store.Identifier}")
                {
                    MentorNode = responsibleNode
                };
                tasks.Add(AddWatcherToReplicationTopology(fromStore, databaseWatcher, store.Urls));
            }
            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                resList.Add(await task);
            }
            return resList;
        }


        public class ReplicationController : IDisposable
        {
            private readonly DocumentDatabase _database;
            private readonly ManualResetEventSlim _mre;
            public ReplicationController(DocumentDatabase database, bool passSingleTx = true)
            {
                _database = database;
                if (passSingleTx)
                    database.Configuration.Replication.MaxItemsCount = 1;

                database.ReplicationLoader.DebugWaitAndRunReplicationOnce ??= new ManualResetEventSlim(true);
                _mre = database.ReplicationLoader.DebugWaitAndRunReplicationOnce;
            }
            public void ReplicateOnce()
            {
                WaitForReset();
                _mre.Set();
            }

            private void WaitForReset(int timeout = 15_000)
            {
                var sp = Stopwatch.StartNew();
                while (sp.ElapsedMilliseconds < timeout)
                {
                    if (_mre.IsSet == false)
                        return;

                    Thread.Sleep(16);
                }

                throw new TimeoutException();
            }

            public void Dispose()
            {
                WaitForReset();
                _database.ReplicationLoader.DebugWaitAndRunReplicationOnce = null;
                _mre.Set();
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

        protected static async Task SetupReplicationWithCustomDestinations(DocumentStore fromStore, params ReplicationNode[] toNodes)
        {
            foreach (var node in toNodes)
            {
                var databaseWatcher = new ExternalReplication(node.Database, "connection");
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

        protected async Task<(DocumentStore source, DocumentStore destination)> CreateDuoCluster(Options options, [CallerMemberName] string caller = null)
        {
            var (_, leader) = await CreateRaftCluster(2);
            var follower = Servers.First(srv => ReferenceEquals(srv, leader) == false);

            options.ReplicationFactor = 2;
            options.ModifyDatabaseName = _ => caller;
            options.ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true };
            
            options.Server = leader;
            var dstOptions = options.Clone();
            dstOptions.Server = follower;
            dstOptions.CreateDatabase = false;

            var source = GetDocumentStore(options);
            var destination = GetDocumentStore(dstOptions);
         
            return (source, destination);
        }

        protected static async Task<int> GetPromotableCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Promotables.Count;
        }

        protected static async Task<int> GetRehabCount(IDocumentStore store, string databaseName = null)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName ?? store.Database));
            if (res == null)
            {
                return -1;
            }
            return res.IsSharded ? res.Sharding.Orchestrator.Topology.Rehabs.Count : res.Topology.Rehabs.Count;
        }

        protected static async Task<int> GetMembersCount(IDocumentStore store, string databaseName = null)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName ?? store.Database));
            if (res == null)
            {
                return -1;
            }
            return res.IsSharded ? res.Sharding.Orchestrator.Topology.Members.Count : res.Topology.Members.Count;
        }

        protected static async Task<int> GetDeletionCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.DeletionInProgress.Count;
        }

        protected Options UpdateConflictSolverAndGetMergedOptions(Options options)
        {
            return new Options(options)
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                    options.ModifyDatabaseRecord(record);
                }
            };
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

        protected internal class GetReplicationTombstonesCommand : RavenCommand<List<string>>
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
                if (response.TryGet(nameof(GetTombstonesPreviewResult.Tombstones), out array) == false)
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

        public class NoChangeVectorContext : IChangeVectorOperationContext
        {
            public static NoChangeVectorContext Instance = new NoChangeVectorContext();

            public ChangeVector GetChangeVector(string changeVector, bool throwOnRecursion = false) => new ChangeVector(changeVector, throwOnRecursion, this);

            public ChangeVector GetChangeVector(string version, string order)
            {
                return new ChangeVector(new ChangeVector(version, throwOnRecursion: true, this), 
                    new ChangeVector(version, throwOnRecursion: true, this));
            }
        }
    }

    public static class ChangeVectorTestExtensions
    {
        public static ChangeVector ToVersion(this string changeVector) => new ChangeVector(changeVector, ReplicationTestBase.NoChangeVectorContext.Instance).Version;
    }
}
