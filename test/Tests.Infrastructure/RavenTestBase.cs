using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests
{
    public class RavenTestBase : TestBase
    {
        protected readonly ConcurrentSet<DocumentStore> CreatedStores = new ConcurrentSet<DocumentStore>();

        protected virtual Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(IDocumentStore store)
        {
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
        }

        protected async Task SetDatabaseId(DocumentStore store, Guid dbId)
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            var type = database.GetAllStoragesEnvironment().Single(t => t.Type == StorageEnvironmentWithType.StorageEnvironmentType.Documents);
            type.Environment.DbId = dbId;
        }

        private readonly object _getDocumentStoreSync = new object();

        protected async Task WaitForRaftIndexToBeAppliedInCluster(long index, TimeSpan timeout)
        {
            if (Servers.Count == 0)
                return;

            var tasks = Servers
                .Select(server => server.ServerStore.Cluster.WaitForIndexNotification(index))
                .ToList();

            if (await Task.WhenAll(tasks).WaitAsync(timeout))
                return;

            var message = $"Timed out waiting for {index} after {timeout} because out of {Servers.Count} " +
                          " we got confirmations that it was applied only on to following servers: ";

            for (var i = 0; i < tasks.Count; i++)
            {
                message += $"{Environment.NewLine}Url: {Servers[i].WebUrls[0]}. Applied: {tasks[i].IsCompleted}.";
                if (tasks[i].IsCompleted == false)
                {
                    using (Servers[i].ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        context.OpenReadTransaction();
                        message += $"{Environment.NewLine}Log state for non responsing server:{Environment.NewLine}{context.ReadObject(Servers[i].ServerStore.GetLogDetails(context), "LogSummary/" + i)}";
                    }
                }
            }

            throw new TimeoutException(message);
        }

        protected virtual DocumentStore GetDocumentStore(
            [CallerMemberName] string caller = null,
            string dbSuffixIdentifier = null,
            string path = null,
            Action<DatabaseRecord> modifyDatabaseRecord = null,
            Func<string, string> modifyName = null,
            bool ignoreDisabledDatabase = false,
            int replicationFactor = 1,
            RavenServer defaultServer = null,
            bool waitForDatabasesToBeCreated = false,
            bool deleteDatabaseWhenDisposed = true,
            bool createDatabase = true,
            X509Certificate2 adminCertificate = null,
            X509Certificate2 userCertificate = null)
        {
            try
            {

                lock (_getDocumentStoreSync)
                {
                    defaultServer = defaultServer ?? Server;
                    var name = GetDatabaseName(caller);

                    if (dbSuffixIdentifier != null)
                        name = $"{name}_{dbSuffixIdentifier}";

                    if (modifyName != null)
                        name = modifyName(name) ?? name;

                    var hardDelete = true;
                    var runInMemory = true;

                    if (path == null)
                        path = NewDataPath(name);
                    else
                    {
                        hardDelete = false;
                        runInMemory = false;
                    }

                    var doc = new DatabaseRecord(name)
                    {
                        Settings =
                        {
                            [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = runInMemory.ToString(),
                            [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = path,
                            [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)] = "true",
                            [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                        }
                    };

                    modifyDatabaseRecord?.Invoke(doc);


                    var store = new DocumentStore
                    {
                        Urls = UseFiddler(defaultServer.WebUrls),
                        Database = name,
                        Certificate = userCertificate
                    };
                    ModifyStore(store);
                    store.Initialize();

                    if (createDatabase)
                    {
                        foreach (var server in Servers)
                        {
                            using (server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                            {
                                context.OpenReadTransaction();
                                if (server.ServerStore.Cluster.Read(context, Constants.Documents.Prefix + name) != null)
                                    throw new InvalidOperationException($"Database '{name}' already exists");
                            }
                        }

                        DatabasePutResult result;

                        if (userCertificate != null)
                        {
                            using (var adminStore = new DocumentStore
                            {
                                Urls = UseFiddler(defaultServer.WebUrls),
                                Database = name,
                                Certificate = adminCertificate
                            }.Initialize())
                            {
                                result = adminStore.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));
                            }
                        }
                        else
                        {
                            result = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));
                        }

                        Assert.True(result.RaftCommandIndex > 0); //sanity check             
                        store.Urls = result.NodesAddedTo.ToArray();
                        var timeout = TimeSpan.FromMinutes(Debugger.IsAttached ? 5 : 1);
                        var task = WaitForRaftIndexToBeAppliedInCluster(result.RaftCommandIndex, timeout);
                        task.ConfigureAwait(false).GetAwaiter().GetResult();
                    }

                    store.AfterDispose += (sender, args) =>
                    {
                        if (CreatedStores.TryRemove(store) == false)
                            return; // can happen if we are wrapping the store inside sharded one

                        foreach (var server in Servers)
                        {
                            if (server.Disposed)
                                continue;

                            if (store.Urls.Any(url => server.WebUrls.Contains(url)) == false)
                                continue;

                            try
                            {
                                var databaseTask = server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name, ignoreDisabledDatabase);
                                if (databaseTask != null && databaseTask.IsCompleted == false)
                                    // if we are disposing store before database had chance to load then we need to wait
                                    databaseTask.Wait();
                            }
                            catch (DatabaseDisabledException)
                            {
                                continue;
                            }

                            if (deleteDatabaseWhenDisposed)
                            {
                                DeleteDatabaseResult result;
                                try
                                {

                                    if (userCertificate != null)
                                    {
                                        using (var adminStore = new DocumentStore
                                        {
                                            Urls = UseFiddler(defaultServer.WebUrls),
                                            Database = name,
                                            Certificate = adminCertificate
                                        }.Initialize())
                                        {
                                            result = adminStore.Admin.Server.Send(new DeleteDatabaseOperation(name, hardDelete));
                                        }
                                    }
                                    else
                                    {
                                        result = store.Admin.Server.Send(new DeleteDatabaseOperation(name, hardDelete));
                                    }

                                }
                                catch (DatabaseDoesNotExistException)
                                {
                                    continue;
                                }
                                catch (NoLeaderException)
                                {
                                    continue;
                                }

                                server.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex).ConfigureAwait(false).GetAwaiter().GetResult();
                            }
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

        protected virtual void ModifyStore(DocumentStore store)
        {
            store.RequestExecutorCreated += (sender, executor) =>
            {
                executor.AdditionalErrorInformation += sb => sb.AppendLine().Append(GetLastStatesFromAllServersOrderedByTime());
            };
        }

        public static void WaitForIndexing(IDocumentStore store, string dbName = null, TimeSpan? timeout = null)
        {
            var admin = store.Admin.ForDatabase(dbName);

            timeout = timeout ?? (Debugger.IsAttached
                          ? TimeSpan.FromMinutes(15)
                          : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var databaseStatistics = admin.Send(new GetStatisticsOperation());
                var indexes = databaseStatistics.Indexes
                    .Where(x => x.State != IndexState.Disabled);

                if (indexes.All(x => x.IsStale == false && x.Name.StartsWith("ReplacementOf/") == false))
                    return;

                if (databaseStatistics.Indexes.Any(x => x.State == IndexState.Error))
                {
                    break;
                }
                Thread.Sleep(32);
            }

            var perf = admin.Send(new GetIndexPerformanceStatisticsOperation());
            var errors = admin.Send(new GetIndexErrorsOperation());
            var stats = admin.Send(new GetIndexesStatisticsOperation());

            var total = new
            {
                Errors = errors,
                Stats = stats,
                Performance = perf
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

            var statistics = admin.Send(new GetStatisticsOperation());

            var corrupted = statistics.Indexes.Where(x => x.State == IndexState.Error).ToList();
            if (corrupted.Count > 0)
            {
                throw new InvalidOperationException(
                    $"The following indexes are with error state: {string.Join(",", corrupted.Select(x => x.Name))} - details at " + file);
            }

            throw new TimeoutException("The indexes stayed stale for more than " + timeout.Value + ", stats at " + file);
        }


        protected async Task<T> WaitForValueAsync<T>(Func<Task<T>> act, T expectedVal, int timeout = 15000)
        {
            if (Debugger.IsAttached)
                timeout *= 100;
            var sw = Stopwatch.StartNew();
            do
            {
                try
                {
                    var currentVal = await act();
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
                    if (sw.ElapsedMilliseconds <= timeout)
                    {
                        throw;
                    }
                }
                await Task.Delay(100);
            } while (true);
        }

        protected async Task<T> WaitForValueAsync<T>(Func<T> act, T expectedVal)
        {
            int timeout = 5000;// * (Debugger.IsAttached ? 100 : 1);

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


        protected T WaitForValue<T>(Func<T> act, T expectedVal)
        {
            int timeout = 15000;
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

                Thread.Sleep(16);
            } while (true);
        }

        public static void WaitForUserToContinueTheTest(string url, bool debug = true, int port = 8079)
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

        public static void WaitForUserToContinueTheTest(DocumentStore documentStore, bool debug = true, int port = 8079)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            var urls = documentStore.Urls;

            var databaseNameEncoded = Uri.EscapeDataString(documentStore.Database);
            var documentsPage = urls.First() + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

            OpenBrowser(documentsPage);// start the server

            do
            {
                Thread.Sleep(500);
            } while (documentStore.Commands().Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
        }

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var store in CreatedStores)
                exceptionAggregator.Execute(store.Dispose);
            CreatedStores.Clear();
        }

        protected X509Certificate2 CreateAndPutClientCertificate(string serverCertPath,
            RavenServer.CertificateHolder serverCertificateHolder,
            Dictionary<string, DatabaseAccess> permissions,
            bool serverAdmin = false,
            RavenServer defaultServer = null)
        {
            var clientCertificate = CertificateUtils.CreateSelfSignedClientCertificate("RavenTestsClient", serverCertificateHolder);
            var serverCertificate = new X509Certificate2(serverCertPath);
            using (var store = GetDocumentStore(adminCertificate: serverCertificate, defaultServer: defaultServer))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new PutClientCertificateOperation(clientCertificate, permissions, serverAdmin)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                }
            }
            return clientCertificate;
        }

        protected X509Certificate2 AskServerForClientCertificate(string serverCertPath, Dictionary<string, DatabaseAccess> permissions, bool serverAdmin = false, RavenServer defaultServer = null)
        {
            var serverCertificate = new X509Certificate2(serverCertPath);
            X509Certificate2 clientCertificate;

            using (var store = GetDocumentStore(adminCertificate: serverCertificate, userCertificate: serverCertificate, defaultServer: defaultServer))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new CreateClientCertificateOperation("client certificate", permissions, serverAdmin)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                    clientCertificate = new X509Certificate2(command.Result.RawData);
                }
            }
            return clientCertificate;
        }

        protected string SetupServerAuthentication(
            IDictionary<string, string> customSettings = null,
            string serverUrl = null,
            bool doNotReuseServer = true)
        {
            var serverCertPath = GenerateAndSaveSelfSignedCertificate();

            if (customSettings == null)
                customSettings = new ConcurrentDictionary<string, string>();

            customSettings[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = serverCertPath;
            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrl)] = serverUrl ?? "https://" + Environment.MachineName + ":0";

            if (doNotReuseServer)
                DoNotReuseServer(customSettings);

            return serverCertPath;
        }
    }
}
