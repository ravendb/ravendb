using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Graph;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests
{
    public abstract class RavenTestBase : TestBase
    {
        protected readonly ConcurrentSet<DocumentStore> CreatedStores = new ConcurrentSet<DocumentStore>();

        protected RavenTestBase(ITestOutputHelper output) : base(output)
        {
        }

        protected virtual Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(IDocumentStore store, string database = null)
        {
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database ?? store.Database);
        }

        protected static void CreateNorthwindDatabase(DocumentStore store, DatabaseItemType operateOnTypes = DatabaseItemType.Documents)
        {
            store.Maintenance.Send(new CreateSampleDataOperation(operateOnTypes));
        }

        protected async Task CreateLegacyNorthwindDatabase(DocumentStore store)
        {
            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream("Tests.Infrastructure.Data.Northwind.4.2.ravendbdump"))
            {
                Assert.NotNull(stream);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
        }

        protected async Task SetDatabaseId(DocumentStore store, Guid dbId)
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            var type = database.GetAllStoragesEnvironment().Single(t => t.Type == StorageEnvironmentWithType.StorageEnvironmentType.Documents);
            type.Environment.FillBase64Id(dbId);
        }

        private readonly object _getDocumentStoreSync = new object();

        protected string EncryptedServer(out TestCertificatesHolder certificates, out string name)
        {
            certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName();
            RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }

            var base64Key = Convert.ToBase64String(buffer);

            var canUseProtect = PlatformDetails.RunningOnPosix == false;

            if (canUseProtect)
            {
                // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
                try
                {
                    ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
                }
                catch (PlatformNotSupportedException)
                {
                    canUseProtect = false;
                }
            }

            if (canUseProtect == false) // fall back to a file
                Server.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();

            Assert.True(Server.ServerStore.EnsureNotPassiveAsync().Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);
            name = dbName;
            return Convert.ToBase64String(buffer);
        }

        protected async Task WaitForRaftCommandToBeAppliedInCluster(RavenServer leader, string commandType)
        {
            var updateIndex = LastRaftIndexForCommand(leader, commandType);
            await WaitForRaftIndexToBeAppliedInCluster(updateIndex, TimeSpan.FromSeconds(10));
        }

        protected async Task WaitForRaftCommandToBeAppliedInLocalServer(string commandType)
        {
            var updateIndex = LastRaftIndexForCommand(Server, commandType);
            await Server.ServerStore.Cluster.WaitForIndexNotification(updateIndex, TimeSpan.FromSeconds(10));
        }

        protected long LastRaftIndexForCommand(RavenServer server, string commandType)
        {
            var updateIndex = 0L;
            var commandFound = false;
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var entry in server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                {
                    var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                    if (type == commandType)
                    {
                        commandFound = true;
                        Assert.True(long.TryParse(entry[nameof(RachisLogHistory.LogHistoryColumn.Index)].ToString(), out updateIndex));
                    }
                }
            }

            Assert.True(commandFound, $"{commandType} wasn't found in the log.");
            return updateIndex;
        }

        protected IEnumerable<DynamicJsonValue> GetRaftCommands(RavenServer server, string commandType = null)
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var entry in server.ServerStore.Engine.LogHistory.GetHistoryLogs(context))
                {
                    var type = entry[nameof(RachisLogHistory.LogHistoryColumn.Type)].ToString();
                    if (commandType == null || commandType == type)
                        yield return entry;
                }
            }
        }

        protected async Task WaitForRaftIndexToBeAppliedInCluster(long index, TimeSpan? timeout = null)
        {
            await WaitForRaftIndexToBeAppliedOnClusterNodes(index, Servers, timeout);
        }

        protected async Task WaitForRaftIndexToBeAppliedOnClusterNodes(long index, List<RavenServer> nodes, TimeSpan? timeout = null)
        {
            if (nodes.Count == 0)
                throw new InvalidOperationException("Cannot wait for raft index to be applied when the cluster is empty. Make sure you are using the right server.");

            if (timeout.HasValue == false)
                timeout = Debugger.IsAttached ? TimeSpan.FromSeconds(300) : TimeSpan.FromSeconds(60);

            var tasks = nodes.Where(s => s.ServerStore.Disposed == false &&
                                          s.ServerStore.Engine.CurrentState != RachisState.Passive)
                .Select(server => server.ServerStore.Cluster.WaitForIndexNotification(index))
                .ToList();

            if (await Task.WhenAll(tasks).WaitAsync(timeout.Value))
                return;

            ThrowTimeoutException(nodes, tasks, index, timeout.Value);
        }

        private void ThrowTimeoutException(List<RavenServer> nodes, List<Task> tasks, long index, TimeSpan timeout)
        {
            var message = $"Timed out after {timeout} waiting for index {index} because out of {nodes.Count} servers" +
                          " we got confirmations that it was applied only on the following servers: ";

            for (var i = 0; i < tasks.Count; i++)
            {
                message += $"{Environment.NewLine}Url: {nodes[i].WebUrl}. Applied: {tasks[i].IsCompleted}.";
                if (tasks[i].IsCompleted == false)
                {
                    using (nodes[i].ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        context.OpenReadTransaction();
                        message += $"{Environment.NewLine}Log state for non responsing server:{Environment.NewLine}{context.ReadObject(nodes[i].ServerStore.GetLogDetails(context), "LogSummary/" + i)}";
                    }
                }
            }

            throw new TimeoutException(message);
        }

        protected virtual DocumentStore GetDocumentStore(Options options = null, [CallerMemberName] string caller = null)
        {
            try
            {
                lock (_getDocumentStoreSync)
                {
                    options = options ?? Options.Default;
                    var serverToUse = options.Server ?? Server;

                    var name = GetDatabaseName(caller);

                    if (options.ModifyDatabaseName != null)
                        name = options.ModifyDatabaseName(name) ?? name;

                    var hardDelete = true;
                    var runInMemory = options.RunInMemory;

                    var pathToUse = options.Path;
                    if (runInMemory == false && options.ReplicationFactor > 1)
                    {
                        if (pathToUse == null)
                        {
                            // the folders will be assigned automatically
                        }
                        else
                        {
                            throw new InvalidOperationException($"You cannot set {nameof(Options)}.{nameof(Options.Path)} when, {nameof(Options)}.{nameof(Options.ReplicationFactor)} > 1 and {nameof(Options)}.{nameof(Options.RunInMemory)} == false.");
                        }
                    }
                    else if (pathToUse == null)
                    {
                        pathToUse = NewDataPath(name);
                    }
                    else
                    {
                        hardDelete = false;
                        runInMemory = false;
                    }

                    var doc = new DatabaseRecord(name)
                    {
                        Settings =
                        {
                            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = runInMemory.ToString(),
                            [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true",
                            [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString(),
                        }
                    };

                    if (pathToUse != null)
                    {
                        doc.Settings.Add(RavenConfiguration.GetKey(x => x.Core.DataDirectory), pathToUse);
                    }

                    if (options.Encrypted)
                    {
                        SetupForEncryptedDatabase(options, name, serverToUse, doc);
                    }

                    options.ModifyDatabaseRecord?.Invoke(doc);

                    var store = new DocumentStore
                    {
                        Urls = UseFiddler(serverToUse.WebUrl),
                        Database = name,
                        Certificate = options.ClientCertificate,
                        Conventions =
                        {
                            DisableTopologyCache = true
                        }
                    };

                    options.ModifyDocumentStore?.Invoke(store);

                    //This gives too much error details in most cases, we don't need this now
                    store.RequestExecutorCreated += (sender, executor) =>
                    {
                        executor.AdditionalErrorInformation += sb => sb.AppendLine().Append(GetLastStatesFromAllServersOrderedByTime());
                    };

                    store.Initialize();

                    if (options.CreateDatabase)
                    {
                        if (Servers.Contains(serverToUse))
                        {
                            Servers.ForEach(server => CheckIfDatabaseExists(server, name));
                        }
                        else
                        {
                            CheckIfDatabaseExists(serverToUse, name);
                        }

                        long raftCommand;
                        try
                        {
                            if (options.AdminCertificate != null)
                            {
                                using (var adminStore = new DocumentStore
                                {
                                    Urls = UseFiddler(serverToUse.WebUrl),
                                    Database = name,
                                    Certificate = options.AdminCertificate
                                }.Initialize())
                                {
                                    raftCommand = adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(doc, options.ReplicationFactor)).RaftCommandIndex;
                                }
                            }
                            else
                            {
                                raftCommand = store.Maintenance.Server.Send(new CreateDatabaseOperation(doc, options.ReplicationFactor)).RaftCommandIndex;
                            }
                        }
                        catch (ConcurrencyException)
                        {
                            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(name));
                            Assert.Equal(options.ReplicationFactor, record.Topology.ReplicationFactor);
                            raftCommand = record.Etag;
                        }

                        Assert.True(raftCommand > 0); //sanity check

                        if (Servers.Contains(serverToUse))
                        {
                            var timeout = TimeSpan.FromMinutes(Debugger.IsAttached ? 5 : 1);
                            AsyncHelpers.RunSync(async () => await WaitForRaftIndexToBeAppliedInCluster(raftCommand, timeout));

                            // skip 'wait for requests' on DocumentDatabase dispose
                            Servers.ForEach(server => ApplySkipDrainAllRequestsToDatabase(server, name));
                        }
                        else
                        {
                            ApplySkipDrainAllRequestsToDatabase(serverToUse, name);
                        }
                    }

                    store.BeforeDispose += (sender, args) =>
                    {
                        var realException = Context.GetException();
                        try
                        {
                            if (CreatedStores.TryRemove(store) == false)
                                return; // can happen if we are wrapping the store inside sharded one

                            DeleteDatabaseResult result = null;
                            if (options.DeleteDatabaseOnDispose)
                            {
                                result = DeleteDatabase(options, serverToUse, name, hardDelete, store);
                            }

                            if (Servers.Contains(serverToUse) && result != null)
                            {
                                var timeout = options.DeleteTimeout ?? TimeSpan.FromSeconds(Debugger.IsAttached ? 150 : 15);
                                AsyncHelpers.RunSync(async () => await WaitForRaftIndexToBeAppliedInCluster(result.RaftCommandIndex, timeout));
                            }
                        }
                        catch (Exception e)
                        {
                            if (realException != null)
                                throw new AggregateException(realException, e);

                            throw;
                        }
                    };
                    CreatedStores.Add(store);

                    return store;
                }
            }
            catch (TimeoutException te)
            {
                throw new TimeoutException($"{te.Message} {Environment.NewLine} {te.StackTrace}{Environment.NewLine}Servers states:{Environment.NewLine}{GetLastStatesFromAllServersOrderedByTime()}");
            }
        }

        private static void CheckIfDatabaseExists(RavenServer server, string name)
        {
            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                context.OpenReadTransaction();
                if (server.ServerStore.Cluster.Read(context, Constants.Documents.Prefix + name) != null)
                    throw new InvalidOperationException($"Database '{name}' already exists");
            }
        }

        private static void ApplySkipDrainAllRequestsToDatabase(RavenServer serverToUse, string name)
        {
            try
            {
                var documentDatabase = AsyncHelpers.RunSync(async () => await serverToUse.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name));
                documentDatabase.ForTestingPurposesOnly().SkipDrainAllRequests = true;
            }
            catch (DatabaseNotRelevantException)
            {
            }
        }

        private DeleteDatabaseResult DeleteDatabase(Options options, RavenServer serverToUse, string name, bool hardDelete, DocumentStore store)
        {
            try
            {
                if (options.AdminCertificate != null)
                {
                    using (var adminStore =
                        new DocumentStore { Urls = UseFiddler(serverToUse.WebUrl), Database = name, Certificate = options.AdminCertificate }.Initialize())
                    {
                        return adminStore.Maintenance.Server.Send(new DeleteDatabasesOperation(name, hardDelete));
                    }
                }

                return store.Maintenance.Server.Send(new DeleteDatabasesOperation(name, hardDelete));
            }
            catch (OperationCanceledException)
            {
                //failed to delete in time
            }
            catch (TimeoutException)
            {
                //failed to delete in time
            }
            catch (DatabaseDoesNotExistException)
            {
            }
            catch (NoLeaderException)
            {
            }
            catch (Exception e)
            {
                if (e is RavenException && (e.InnerException is TimeoutException || e.InnerException is OperationCanceledException))
                    return null;

                if (Servers.Contains(serverToUse))
                {
                    if (Servers.All(s => s.Disposed))
                        return null;
                }

                if (serverToUse.Disposed)
                    return null;

                throw;
            }
            return null;
        }

        private void SetupForEncryptedDatabase(Options options, string dbName, RavenServer mainServer, DatabaseRecord doc)
        {
            foreach (var server in Servers)
            {
                if (server.Certificate.Certificate == null)
                {
                    throw new InvalidOperationException("Can't generate encrypted database on not secured servers please create server with 'UseSsl = true'");
                }
            }

            var count = Servers.Count;

            Debug.Assert(count >= options.ReplicationFactor);
            Debug.Assert(options.ReplicationFactor > 0);

            var topology = GenerateStaticTopology(options, mainServer);

            var ravenServers = Servers.Where(s => topology.Members.Contains(s.ServerStore.NodeTag)).ToList();

            foreach (var server in ravenServers)
            {
                PutSecrectKeyForDatabaseInServersStore(dbName, server);
            }

            //This is so things will just work, you must provide a client certificate for GetDocumentStore for encrypted database
            EnsureClientCertificateIsProvidedForEncryptedDatabase(options, mainServer);

            doc.Topology = topology;
            doc.Encrypted = true;
        }

        private void EnsureClientCertificateIsProvidedForEncryptedDatabase(Options options, RavenServer mainServer)
        {
            if (options.ClientCertificate == null)
            {
                if (options.AdminCertificate != null)
                {
                    options.ClientCertificate = options.AdminCertificate;
                }
                else
                {
                    var certificates = GenerateAndSaveSelfSignedCertificate();
                    RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin, server: mainServer);
                    options.AdminCertificate = options.ClientCertificate = certificates.ClientCertificate1.Value;
                }
            }
        }

        /// <summary>
        /// Generating a static topology of the requested size.
        /// </summary>
        /// <param name="options">Contains replication factor.</param>
        /// <param name="mainServer">The main server for which we generate the database, must be contained in the topology.</param>
        /// <returns></returns>
        private DatabaseTopology GenerateStaticTopology(Options options, RavenServer mainServer)
        {
            DatabaseTopology topology = new DatabaseTopology();
            var mainTag = mainServer.ServerStore.NodeTag;
            topology.Members.Add(mainTag);
            var rand = new Random();
            var serverTags = Servers.Where(s => s != mainServer).Select(s => s.ServerStore.NodeTag).ToList();

            for (var i = 0; i < options.ReplicationFactor - 1; i++)
            {
                var position = rand.Next(0, serverTags.Count);
                topology.Members.Add(serverTags[position]);
                serverTags.RemoveAt(position);
            }

            return topology;
        }

        protected string GetLastStatesFromAllServersOrderedByTime()
        {
            List<(string tag, RachisConsensus.StateTransition transition)> states = new List<(string tag, RachisConsensus.StateTransition transition)>();
            foreach (var s in Servers)
            {
                foreach (var state in s.ServerStore.Engine.PrevStates)
                {
                    states.Add((s.ServerStore.NodeTag, state));
                }
            }
            return string.Join(Environment.NewLine, states.OrderBy(x => x.transition.When).Select(x => $"State for {x.tag}-term{x.Item2.CurrentTerm}:{Environment.NewLine}{x.Item2.From}=>{x.Item2.To} at {x.Item2.When:o} {Environment.NewLine}because {x.Item2.Reason}"));
        }

        public static void WaitForIndexing(IDocumentStore store, string dbName = null, TimeSpan? timeout = null, bool allowErrors = false, string nodeTag = null)
        {
            var admin = store.Maintenance.ForDatabase(dbName);

            timeout ??= (Debugger.IsAttached
                ? TimeSpan.FromMinutes(15)
                : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var databaseStatistics = admin.Send(new GetStatisticsOperation("wait-for-indexing", nodeTag));
                var indexes = databaseStatistics.Indexes
                    .Where(x => x.State != IndexState.Disabled);

                var staleIndexesCount = indexes.Count(x => x.IsStale || x.Name.StartsWith("ReplacementOf/"));
                if (staleIndexesCount == 0)
                    return;

                var erroredIndexesCount = databaseStatistics.Indexes.Count(x => x.State == IndexState.Error);
                if (allowErrors)
                {
                    // wait for all indexes to become non stale
                }
                else if (erroredIndexesCount > 0)
                {
                    // have at least some errors
                    break;
                }

                Thread.Sleep(32);
            }

            if (allowErrors)
            {
                return;
            }

            var perf = admin.Send(new GetIndexPerformanceStatisticsOperation());
            var errors = admin.Send(new GetIndexErrorsOperation());
            var stats = admin.Send(new GetIndexesStatisticsOperation());

            var total = new
            {
                Errors = errors,
                Stats = stats,
                Performance = perf,
                NodeTag = nodeTag
            };

            var file = Path.GetTempFileName() + ".json";
            using (var stream = File.Open(file, FileMode.OpenOrCreate))
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(total);
                var json = context.ReadObject(djv, "errors");
                writer.WriteObject(json);
                writer.Flush();
            }

            var statistics = admin.Send(new GetStatisticsOperation("wait-for-indexing", nodeTag));

            var corrupted = statistics.Indexes.Where(x => x.State == IndexState.Error).ToList();
            if (corrupted.Count > 0)
            {
                throw new InvalidOperationException(
                    $"The following indexes are with error state: {string.Join(",", corrupted.Select(x => x.Name))} - details at " + file);
            }

            throw new TimeoutException("The indexes stayed stale for more than " + timeout.Value + ", stats at " + file);
        }

        public static IndexErrors[] WaitForIndexingErrors(IDocumentStore store, string[] indexNames = null, TimeSpan? timeout = null)
        {
            timeout ??= (Debugger.IsAttached
                          ? TimeSpan.FromMinutes(15)
                          : TimeSpan.FromMinutes(1));

            var toWait = new HashSet<string>(indexNames ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var indexes = store.Maintenance.Send(new GetIndexErrorsOperation(indexNames));
                foreach (var index in indexes)
                {
                    if (index.Errors.Length > 0)
                    {
                        toWait.Remove(index.Name);

                        if (toWait.Count == 0)
                            return indexes;
                    }
                }

                Thread.Sleep(32);
            }

            var msg = $"Got no index error for more than {timeout.Value}.";
            if (toWait.Count != 0)
                msg += $" Still waiting for following indexes: {string.Join(",", toWait)}";

            throw new TimeoutException(msg);
        }

        public static int WaitForEntriesCount(IDocumentStore store, string indexName, int minEntriesCount, string databaseName = null, TimeSpan? timeout = null, bool throwOnTimeout = true)
        {
            timeout ??= (Debugger.IsAttached
                ? TimeSpan.FromMinutes(15)
                : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            var entriesCount = -1;

            while (sp.Elapsed < timeout.Value)
            {
                MaintenanceOperationExecutor operations = string.IsNullOrEmpty(databaseName) == false ? store.Maintenance.ForDatabase(databaseName) : store.Maintenance;

                entriesCount = operations.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount;

                if (entriesCount >= minEntriesCount)
                    return entriesCount;

                Thread.Sleep(32);
            }

            if (throwOnTimeout)
                throw new TimeoutException($"It didn't get min entries count {minEntriesCount} for index {indexName}. The index has {entriesCount} entries.");

            return entriesCount;
        }

        protected async Task<T> AssertWaitForNotNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class
        {
            var ret = await WaitForNotNullAsync(act, timeout, interval);
            Assert.NotNull(ret);
            return ret;
        }

        protected async Task<T> WaitForNotNullAsync<T>(Func<Task<T>> act, int timeout = 15000, int interval = 100) where T : class =>
            await WaitForPredicateAsync(a => a != null, act, timeout, interval);

        protected async Task<T> WaitForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100)
        {
            return await WaitForPredicateAsync(t => t.Equals(expectedVal), act, timeout, interval);
        }

        protected async Task WaitAndAssertForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000, int interval = 100)
        {
            var val = await WaitForPredicateAsync(t => t.Equals(expectedVal), act, timeout, interval);
            Assert.Equal(expectedVal, val);
        }

        private static async Task<T> WaitForPredicateAsync<T>(Predicate<T> predicate, Func<Task<T>> act, int timeout = 15000, int interval = 100)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            while (true)
            {
                try
                {
                    var currentVal = await act();
                    if (predicate(currentVal) || sw.ElapsedMilliseconds > timeout)
                        return currentVal;
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }
                await Task.Delay(interval);
            }
        }

        protected static async Task<T> WaitForValueAsync<T>(Func<T> act, T expectedVal, int timeout = 15000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            do
            {
                try
                {
                    var currentVal = act();
                    if (expectedVal.Equals(currentVal))
                    {
                        return currentVal;
                    }
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        return currentVal;
                    }
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }
                await Task.Delay(100);
            } while (true);
        }

        protected static T WaitForValue<T>(Func<T> act, T expectedVal, int timeout = 15000, int interval = 16)
        {
            if (Debugger.IsAttached)
                timeout *= 100;

            var sw = Stopwatch.StartNew();
            do
            {
                try
                {
                    var currentVal = act();
                    if (expectedVal.Equals(currentVal))
                    {
                        return currentVal;
                    }
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        return currentVal;
                    }
                }
                catch
                {
                    if (sw.ElapsedMilliseconds > timeout)
                    {
                        throw;
                    }
                }

                Thread.Sleep(interval);
            } while (true);
        }

        public static void WaitForUserToContinueTheTest(string url, bool debug = true)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            var documentsPage = url + "/studio/index.html";

            OpenBrowser(documentsPage);// start the server

            do
            {
                Thread.Sleep(500);
            } while (debug == false || Debugger.IsAttached);
        }

        public static void WaitForUserToContinueTheTest(IDocumentStore documentStore, bool debug = true, string database = null, X509Certificate2 clientCert = null)
        {
            if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                {
                    userPersonalStore.Open(OpenFlags.ReadWrite);
                    userPersonalStore.Add(clientCert);
                }
            }

            try
            {
                if (debug && Debugger.IsAttached == false)
                    return;

                var urls = documentStore.Urls;
                if (clientCert != null)
                    Console.WriteLine($"Using certificate with serial: {clientCert.SerialNumber}");

                var databaseNameEncoded = Uri.EscapeDataString(database ?? documentStore.Database);
                var documentsPage = urls.First() + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

                OpenBrowser(documentsPage);// start the server

                do
                {
                    Thread.Sleep(500);
                } while (documentStore.Commands(database).Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));

                documentStore.Commands(database).Delete("Debug/Done", null);
            }
            finally
            {
                if (clientCert != null && RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    using (var userPersonalStore = new X509Store(StoreName.My, StoreLocation.CurrentUser))
                    {
                        userPersonalStore.Open(OpenFlags.ReadWrite);
                        userPersonalStore.Remove(clientCert);
                    }
                }
            }
        }

        protected ManualResetEventSlim WaitForIndexBatchCompleted(IDocumentStore store, Func<(string IndexName, bool DidWork), bool> predicate)
        {
            var database = GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.IndexStore.IndexBatchCompleted += x =>
            {
                if (predicate(x))
                    mre.Set();
            };

            return mre;
        }

        protected static bool WaitForCounterReplication(IEnumerable<IDocumentStore> stores, string docId, string counterName, long expected, TimeSpan timeout)
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

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var store in CreatedStores)
            {
                if (store.WasDisposed)
                    continue;

                exceptionAggregator.Execute(store.Dispose);
            }
            CreatedStores.Clear();
        }

        protected X509Certificate2 RegisterClientCertificate(TestCertificatesHolder certificates, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser, RavenServer server = null)
        {
            return RegisterClientCertificate(certificates.ServerCertificate.Value, certificates.ClientCertificate1.Value, permissions, clearance, server);
        }

        protected X509Certificate2 RegisterClientCertificate(
            X509Certificate2 serverCertificate,
            X509Certificate2 clientCertificate,
            Dictionary<string, DatabaseAccess> permissions,
            SecurityClearance clearance = SecurityClearance.ValidUser,
            RavenServer server = null,
            string certificateName = "client certificate")
        {
            using var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = server,
                ClientCertificate = serverCertificate,
                AdminCertificate = serverCertificate,
                ModifyDocumentStore = s => s.Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            });
            store.Maintenance.Server.Send(new PutClientCertificateOperation(certificateName, clientCertificate, permissions, clearance));
            return clientCertificate;
        }

        protected IDisposable RestoreDatabase(IDocumentStore store, RestoreBackupConfiguration config, TimeSpan? timeout = null)
        {
            var restoreOperation = new RestoreBackupOperation(config);

            var operation = store.Maintenance.Server.Send(restoreOperation);
            operation.WaitForCompletion(timeout ?? TimeSpan.FromSeconds(30));

            return EnsureDatabaseDeletion(config.DatabaseName, store);
        }

        protected IDisposable RestoreDatabaseFromCloud(IDocumentStore store, RestoreBackupConfigurationBase config, TimeSpan? timeout = null)
        {
            var restoreOperation = new RestoreBackupOperation(config);

            var operation = store.Maintenance.Server.Send(restoreOperation);
            operation.WaitForCompletion(timeout ?? TimeSpan.FromSeconds(30));

            return EnsureDatabaseDeletion(config.DatabaseName, store);
        }

        protected IDisposable EnsureDatabaseDeletion(string databaseToDelete, IDocumentStore store)
        {
            return new DisposableAction(() =>
            {
                try
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseToDelete, hardDelete: true));
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Failed to delete '{databaseToDelete}' database. Exception: " + e);

                    // do not throw to not hide an exception that could be thrown in a test
                }
            });
        }

        protected TestCertificatesHolder SetupServerAuthentication(IDictionary<string, string> customSettings = null, string serverUrl = null, TestCertificatesHolder certificates = null)
        {
            if (customSettings == null)
                customSettings = new ConcurrentDictionary<string, string>();

            if (certificates == null)
                certificates = GenerateAndSaveSelfSignedCertificate();

            if (customSettings.TryGetValue(RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec), out var _) == false)
                customSettings[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = certificates.ServerCertificatePath;

            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl ?? "https://" + Environment.MachineName + ":0";

            DoNotReuseServer(customSettings);

            return certificates;
        }

        private readonly Dictionary<(RavenServer Server, string Database), string> _serverDatabaseToMasterKey = new Dictionary<(RavenServer Server, string Database), string>();

        protected void PutSecrectKeyForDatabaseInServersStore(string dbName, RavenServer server)
        {
            var base64key = CreateMasterKey(out _);
            var base64KeyClone = new string(base64key.ToCharArray());
            EnsureServerMasterKeyIsSetup(server);
            Assert.True(server.ServerStore.EnsureNotPassiveAsync().Wait(TimeSpan.FromSeconds(30))); // activate license so we can insert the secret key
            server.ServerStore.PutSecretKey(base64key, dbName, true);
            _serverDatabaseToMasterKey.Add((server, dbName), base64KeyClone);
        }

        protected string SetupEncryptedDatabase(out TestCertificatesHolder certificates, out byte[] masterKey, [CallerMemberName] string caller = null)
        {
            certificates = SetupServerAuthentication();
            var dbName = GetDatabaseName(caller);
            RegisterClientCertificate(certificates, new Dictionary<string, DatabaseAccess>(), SecurityClearance.ClusterAdmin);

            string base64Key = CreateMasterKey(out masterKey);

            EnsureServerMasterKeyIsSetup(Server);

            Server.ServerStore.PutSecretKey(base64Key, dbName, true);
            return dbName;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureServerMasterKeyIsSetup(RavenServer ravenServer)
        {
            // sometimes when using `dotnet xunit` we get platform not supported from ProtectedData
            try
            {
                ProtectedData.Protect(Encoding.UTF8.GetBytes("Is supported?"), null, DataProtectionScope.CurrentUser);
            }
            catch (PlatformNotSupportedException)
            {
                // so we fall back to a file
                if (File.Exists(ravenServer.ServerStore.Configuration.Security.MasterKeyPath) == false)
                {
                    ravenServer.ServerStore.Configuration.Security.MasterKeyPath = GetTempFileName();
                }
            }
        }

        protected static string CreateMasterKey(out byte[] masterKey)
        {
            var buffer = new byte[32];
            using (var rand = RandomNumberGenerator.Create())
            {
                rand.GetBytes(buffer);
            }

            masterKey = buffer;

            var base64Key = Convert.ToBase64String(buffer);
            return base64Key;
        }

        public class Options
        {
            private readonly bool _frozen;

            private X509Certificate2 _clientCertificate;
            private X509Certificate2 _adminCertificate;
            private bool _createDatabase;
            private bool _deleteDatabaseOnDispose;
            private TimeSpan? _deleteTimeout;
            private RavenServer _server;
            private int _replicationFactor;
            private bool _ignoreDisabledDatabase;
            private Action<DocumentStore> _modifyDocumentStore;
            private Action<DatabaseRecord> _modifyDatabaseRecord;
            private Func<string, string> _modifyDatabaseName;
            private string _path;
            private bool _runInMemory = true;
            private bool _encrypted;

            public static readonly Options Default = new Options(true);

            public Options() : this(false)
            {
            }

            private Options(bool frozen)
            {
                DeleteDatabaseOnDispose = true;
                CreateDatabase = true;
                ReplicationFactor = 1;

                _frozen = frozen;
            }

            public string Path
            {
                get => _path;
                set
                {
                    AssertNotFrozen();
                    _path = value;
                }
            }

            public Func<string, string> ModifyDatabaseName
            {
                get => _modifyDatabaseName;
                set
                {
                    AssertNotFrozen();
                    _modifyDatabaseName = value;
                }
            }

            public Action<DatabaseRecord> ModifyDatabaseRecord
            {
                get => _modifyDatabaseRecord;
                set
                {
                    AssertNotFrozen();
                    _modifyDatabaseRecord = value;
                }
            }

            public Action<DocumentStore> ModifyDocumentStore
            {
                get => _modifyDocumentStore;
                set
                {
                    AssertNotFrozen();
                    _modifyDocumentStore = value;
                }
            }

            public bool IgnoreDisabledDatabase
            {
                get => _ignoreDisabledDatabase;
                set
                {
                    AssertNotFrozen();
                    _ignoreDisabledDatabase = value;
                }
            }

            public int ReplicationFactor
            {
                get => _replicationFactor;
                set
                {
                    AssertNotFrozen();
                    _replicationFactor = value;
                }
            }

            public RavenServer Server
            {
                get => _server;
                set
                {
                    AssertNotFrozen();
                    _server = value;
                }
            }

            public bool DeleteDatabaseOnDispose
            {
                get => _deleteDatabaseOnDispose;
                set
                {
                    AssertNotFrozen();
                    _deleteDatabaseOnDispose = value;
                }
            }

            public TimeSpan? DeleteTimeout
            {
                get => _deleteTimeout;
                set
                {
                    AssertNotFrozen();
                    _deleteTimeout = value;
                }
            }

            public bool CreateDatabase
            {
                get => _createDatabase;
                set
                {
                    AssertNotFrozen();
                    _createDatabase = value;
                    if (value == false)
                    {
                        ModifyDocumentStore = s => s.Conventions.DisableTopologyUpdates = true;
                    }
                }
            }

            public bool RunInMemory
            {
                get => _runInMemory;
                set
                {
                    AssertNotFrozen();
                    _runInMemory = value;
                }
            }

            public X509Certificate2 AdminCertificate
            {
                get => _adminCertificate;
                set
                {
                    AssertNotFrozen();
                    _adminCertificate = value;
                }
            }

            public X509Certificate2 ClientCertificate
            {
                get => _clientCertificate;
                set
                {
                    AssertNotFrozen();
                    _clientCertificate = value;
                }
            }

            public bool Encrypted
            {
                get => _encrypted;
                set
                {
                    AssertNotFrozen();
                    _encrypted = value;
                }
            }

            private void AssertNotFrozen()
            {
                if (_frozen)
                    throw new InvalidOperationException("Options are frozen and cannot be changed.");
            }
        }

        public static async Task WaitForPolicyRunner(DocumentDatabase database)
        {
            var loops = 10;
            await database.TimeSeriesPolicyRunner.HandleChanges();
            for (int i = 0; i < loops; i++)
            {
                var rolled = await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
                if (rolled == 0)
                    return;
            }

            Assert.True(false, $"We still have pending rollups left.");
        }

        protected void CreateSimpleData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var entityA = new Entity { Id = "entity/1", Name = "A" };
                var entityB = new Entity { Id = "entity/2", Name = "B" };
                var entityC = new Entity { Id = "entity/3", Name = "C" };

                session.Store(entityA);
                session.Store(entityB);
                session.Store(entityC);

                entityA.References = entityB.Id;
                entityB.References = entityC.Id;
                entityC.References = entityA.Id;

                session.SaveChanges();
            }
        }

        protected void CreateDogDataWithCycle(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var arava = new Dog { Name = "Arava" }; //dogs/1
                var oscar = new Dog { Name = "Oscar" }; //dogs/2
                var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                arava.Likes = new[] { oscar.Id };
                oscar.Likes = new[] { pheobe.Id };
                pheobe.Likes = new[] { arava.Id };

                session.SaveChanges();
            }
        }

        protected void CreateDogDataWithoutEdges(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var arava = new Dog { Name = "Arava" }; //dogs/1
                var oscar = new Dog { Name = "Oscar" }; //dogs/2
                var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                session.SaveChanges();
            }
        }

        protected void CreateDataWithMultipleEdgesOfTheSameType(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var arava = new Dog { Name = "Arava" }; //dogs/1
                var oscar = new Dog { Name = "Oscar" }; //dogs/2
                var pheobe = new Dog { Name = "Pheobe" }; //dogs/3

                session.Store(arava);
                session.Store(oscar);
                session.Store(pheobe);

                //dogs/1 => dogs/2
                arava.Likes = new[] { oscar.Id };
                arava.Dislikes = new[] { pheobe.Id };

                //dogs/2 => dogs/2,dogs/3 (cycle!)
                oscar.Likes = new[] { oscar.Id, pheobe.Id };
                oscar.Dislikes = new string[0];

                //dogs/3 => dogs/2
                pheobe.Likes = new[] { oscar.Id };
                pheobe.Dislikes = new[] { arava.Id };

                session.SaveChanges();
            }
        }

        protected void CreateMoviesData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var scifi = new Genre
                {
                    Id = "genres/1",
                    Name = "Sci-Fi"
                };

                var fantasy = new Genre
                {
                    Id = "genres/2",
                    Name = "Fantasy"
                };

                var adventure = new Genre
                {
                    Id = "genres/3",
                    Name = "Adventure"
                };

                session.Store(scifi);
                session.Store(fantasy);
                session.Store(adventure);

                var starwars = new Movie
                {
                    Id = "movies/1",
                    Name = "Star Wars Ep.1",
                    Genres = new List<string>
                    {
                        "genres/1",
                        "genres/2"
                    }
                };

                var firefly = new Movie
                {
                    Id = "movies/2",
                    Name = "Firefly Serenity",
                    Genres = new List<string>
                    {
                        "genres/2",
                        "genres/3"
                    }
                };

                var indianaJones = new Movie
                {
                    Id = "movies/3",
                    Name = "Indiana Jones and the Temple Of Doom",
                    Genres = new List<string>
                    {
                        "genres/3"
                    }
                };

                session.Store(starwars);
                session.Store(firefly);
                session.Store(indianaJones);

                session.Store(new User
                {
                    Id = "users/1",
                    Name = "Jack",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/1",
                            Score = 5
                        },
                        new User.Rating
                        {
                            Movie = "movies/2",
                            Score = 7
                        }
                    }
                });

                session.Store(new User
                {
                    Id = "users/2",
                    Name = "Jill",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/2",
                            Score = 7
                        },
                        new User.Rating
                        {
                            Movie = "movies/3",
                            Score = 9
                        }
                    }
                });

                session.Store(new User
                {
                    Id = "users/3",
                    Name = "Bob",
                    HasRated = new List<User.Rating>
                    {
                        new User.Rating
                        {
                            Movie = "movies/3",
                            Score = 5
                        }
                    }
                });

                session.SaveChanges();
            }
        }

        protected void SaveChangesWithTryCatch<T>(IDocumentSession session, T loaded) where T : class
        {
            //This try catch is only to investigate RavenDB-15366 issue
            try
            {
                session.SaveChanges();
            }
            catch (Exception e)
            {
                if (!(session is InMemoryDocumentSessionOperations inMemoryDocumentSessionOperations))
                    throw;

                foreach (var entity in inMemoryDocumentSessionOperations.DocumentsByEntity)
                {
                    if (!(entity.Key is T t) || t != loaded)
                        continue;

                    var blittable = inMemoryDocumentSessionOperations.JsonConverter.ToBlittable(entity.Key, entity.Value);
                    throw new InvalidOperationException($"blittable: {blittable}\n documentInfo {entity.Value.Document}", e);
                }
            }
        }

        protected async Task SaveChangesWithTryCatchAsync<T>(IAsyncDocumentSession session, T loaded) where T : class
        {
            //This try catch is only to investigate RavenDB-15366 issue
            try
            {
                await session.SaveChangesAsync();
            }
            catch (Exception e)
            {
                if (!(session is InMemoryDocumentSessionOperations inMemoryDocumentSessionOperations))
                    throw;

                foreach (var entity in inMemoryDocumentSessionOperations.DocumentsByEntity)
                {
                    if (!(entity.Key is T u) || u != loaded)
                        continue;

                    var blittable = inMemoryDocumentSessionOperations.JsonConverter.ToBlittable(entity.Key, entity.Value);
                    throw new InvalidOperationException($"blittable: {blittable}\n documentInfo {entity.Value.Document}", e);
                }
            }
        }

        protected void WriteDocDirectlyFromStorageToTestOutput(string storeDatabase, string docId, [CallerMemberName] string caller = null)
        {
            AsyncHelpers.RunSync(() => WriteDocDirectlyFromStorageToTestOutputAsync(storeDatabase, docId));
        }

        protected async Task WriteDocDirectlyFromStorageToTestOutputAsync(string storeDatabase, string docId, [CallerMemberName] string caller = null)
        {
            //This function is only to investigate RavenDB-15366 issue

            var db = await GetDatabase(storeDatabase);
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var doc = db.DocumentsStorage.Get(context, docId);

                var sb = new StringBuilder();
                sb.AppendLine($"Test: '{caller}'. Document: '{docId}'. Data:");
                sb.AppendLine(doc.Data.ToString());

                Output?.WriteLine(sb.ToString());
                Console.WriteLine(sb.ToString());
            }
        }
    }
}
