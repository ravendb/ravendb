using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Graph;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
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

        protected virtual Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(IDocumentStore store, string database = null)
        {
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(database ?? store.Database);
        }

        protected static void CreateNorthwindDatabase(DocumentStore store)
        {
            store.Maintenance.Send(new CreateSampleDataOperation());
        }


        protected async Task SetDatabaseId(DocumentStore store, Guid dbId)
        {
            var database = await GetDocumentDatabaseInstanceFor(store);
            var type = database.GetAllStoragesEnvironment().Single(t => t.Type == StorageEnvironmentWithType.StorageEnvironmentType.Documents);
            type.Environment.FillBase64Id(dbId);
        }

        private readonly object _getDocumentStoreSync = new object();

        protected async Task WaitForRaftIndexToBeAppliedInCluster(long index, TimeSpan timeout)
        {
            if (Servers.Count == 0)
                return;

            var tasks = Servers.Where(s => s.ServerStore.Engine.CurrentState != RachisState.Passive)
                .Select(server => server.ServerStore.Cluster.WaitForIndexNotification(index))
                .ToList();

            if (await Task.WhenAll(tasks).WaitAsync(timeout))
                return;

            var message = $"Timed out after {timeout} waiting for index {index} because out of {Servers.Count} servers" +
                          " we got confirmations that it was applied only on the following servers: ";

            for (var i = 0; i < tasks.Count; i++)
            {
                message += $"{Environment.NewLine}Url: {Servers[i].WebUrl}. Applied: {tasks[i].IsCompleted}.";
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
                    var runInMemory = true;

                    var pathToUse = options.Path;
                    if (pathToUse == null)
                        pathToUse = NewDataPath(name);
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
                            [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1",
                            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = runInMemory.ToString(),
                            [RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = pathToUse,
                            [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true",
                            [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString(),
                        }
                    };

                    options.ModifyDatabaseRecord?.Invoke(doc);

                    var store = new DocumentStore
                    {
                        Urls = UseFiddler(serverToUse.WebUrl),
                        Database = name,
                        Certificate = options.ClientCertificate
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

                        if (options.AdminCertificate != null)
                        {
                            using (var adminStore = new DocumentStore
                            {
                                Urls = UseFiddler(serverToUse.WebUrl),
                                Database = name,
                                Certificate = options.AdminCertificate
                            }.Initialize())
                            {
                                result = adminStore.Maintenance.Server.Send(new CreateDatabaseOperation(doc, options.ReplicationFactor));
                            }
                        }
                        else
                        {
                            result = store.Maintenance.Server.Send(new CreateDatabaseOperation(doc, options.ReplicationFactor));
                        }

                        Assert.True(result.RaftCommandIndex > 0); //sanity check             
                        var timeout = TimeSpan.FromMinutes(Debugger.IsAttached ? 5 : 1);
                        AsyncHelpers.RunSync(async () =>
                        {
                            await WaitForRaftIndexToBeAppliedInCluster(result.RaftCommandIndex, timeout);
                        });
                    }

                    store.BeforeDispose += (sender, args) =>
                    {
                        if (CreatedStores.TryRemove(store) == false)
                            return; // can happen if we are wrapping the store inside sharded one

                        foreach (var server in Servers)
                        {
                            if (server.Disposed)
                                continue;
                            var serverUrl = UseFiddler(server.WebUrl);
                            if (store.Urls.Any(url => serverUrl.Contains(url)) == false)
                                continue;

                            try
                            {
                                var databaseTask = server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name, options.IgnoreDisabledDatabase);
                                if (databaseTask != null && databaseTask.IsCompleted == false)
                                    // if we are disposing store before database had chance to load then we need to wait
                                    databaseTask.Wait();
                            }
                            catch (DatabaseDisabledException)
                            {
                                // ignoring
                            }
                            catch (DatabaseNotRelevantException)
                            {
                                continue;
                            }

                            if (options.DeleteDatabaseOnDispose)
                            {
                                DeleteDatabaseResult result;
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
                                            result = adminStore.Maintenance.Server.Send(new DeleteDatabasesOperation(name, hardDelete));
                                        }
                                    }
                                    else
                                    {
                                        result = store.Maintenance.Server.Send(new DeleteDatabasesOperation(name, hardDelete));
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

        public static void WaitForIndexing(IDocumentStore store, string dbName = null, TimeSpan? timeout = null, bool allowErrors = false)
        {
            var admin = store.Maintenance.ForDatabase(dbName);

            timeout = timeout ?? (Debugger.IsAttached
                          ? TimeSpan.FromMinutes(15)
                          : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var databaseStatistics = admin.Send(new GetStatisticsOperation());
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

        public static IndexErrors[] WaitForIndexingErrors(IDocumentStore store, TimeSpan? timeout = null)
        {
            timeout = timeout ?? (Debugger.IsAttached
                          ? TimeSpan.FromMinutes(15)
                          : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var indexes = store.Maintenance.Send(new GetIndexErrorsOperation());
                foreach (var index in indexes)
                {
                    if (index.Errors.Any())
                        return indexes;
                }

                Thread.Sleep(32);
            }

            throw new TimeoutException("Got no index error for more than " + timeout.Value);
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

        protected async Task<T> WaitForValueAsync<T>(Func<T> act, T expectedVal, int timeout = 15000)
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

        public static void WaitForUserToContinueTheTest(IDocumentStore documentStore, bool debug = true, string database = null)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            var urls = documentStore.Urls;

            var databaseNameEncoded = Uri.EscapeDataString(database ?? documentStore.Database);
            var documentsPage = urls.First() + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

            OpenBrowser(documentsPage);// start the server

            do
            {
                Thread.Sleep(500);
            } while (documentStore.Commands(database).Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));

            documentStore.Commands(database).Delete("Debug/Done", null);
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

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var store in CreatedStores)
                exceptionAggregator.Execute(store.Dispose);
            CreatedStores.Clear();
        }

        protected X509Certificate2 CreateAndPutClientCertificate(string serverCertPath,
            RavenServer.CertificateHolder serverCertificateHolder,
            Dictionary<string, DatabaseAccess> permissions,
            SecurityClearance clearance,
            RavenServer server = null)
        {
            var clientCertificate = CertificateUtils.CreateSelfSignedClientCertificate("RavenTestsClient", serverCertificateHolder, out var clietnCertBytes);
            var serverCertificate = new X509Certificate2(serverCertPath, (string)null, X509KeyStorageFlags.MachineKeySet);
            using (var store = GetDocumentStore(new Options
            {
                AdminCertificate = serverCertificate,
                Server = server
            }))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new PutClientCertificateOperation("RavenTestsClient", clientCertificate, permissions, clearance)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                }
            }
            return new X509Certificate2(clietnCertBytes, (string)null, X509KeyStorageFlags.MachineKeySet);
        }

        protected X509Certificate2 AskServerForClientCertificate(string serverCertPath, Dictionary<string, DatabaseAccess> permissions, SecurityClearance clearance = SecurityClearance.ValidUser, RavenServer server = null)
        {
            X509Certificate2 serverCertificate;
            try
            {
                serverCertificate = new X509Certificate2(serverCertPath, (string)null, X509KeyStorageFlags.MachineKeySet);
            }
            catch (CryptographicException e)
            {
                throw new CryptographicException($"Failed to load the test certificate from {serverCertPath}.", e);
            }

            X509Certificate2 clientCertificate;

            using (var store = GetDocumentStore(new Options
            {
                CreateDatabase = false,
                Server = server,
                ClientCertificate = serverCertificate,
                AdminCertificate = serverCertificate,
                ModifyDocumentStore = s=>s.Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }))
            {
                var requestExecutor = store.GetRequestExecutor();
                using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var command = new CreateClientCertificateOperation("client certificate", permissions, clearance)
                        .GetCommand(store.Conventions, context);

                    requestExecutor.Execute(command, context);
                    using (var archive = new ZipArchive(new MemoryStream(command.Result.RawData)))
                    {
                        var entry = archive.Entries.First(e => string.Equals(Path.GetExtension(e.Name), ".pfx", StringComparison.OrdinalIgnoreCase));
                        using (var stream = entry.Open())
                        {
                            var destination = new MemoryStream();
                            stream.CopyTo(destination);
                            clientCertificate = new X509Certificate2(destination.ToArray(), (string)null, X509KeyStorageFlags.MachineKeySet);
                        }
                    }
                }
            }
            return clientCertificate;
        }

        protected IDisposable RestoreDatabase(IDocumentStore store, RestoreBackupConfiguration config, TimeSpan? timeout = null)
        {
            var restoreOperation = new RestoreBackupOperation(config);

            var operation = store.Maintenance.Server.Send(restoreOperation);
            operation.WaitForCompletion(timeout ?? TimeSpan.FromSeconds(55530)); //todo revert

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

        protected string SetupServerAuthentication(
            IDictionary<string, string> customSettings = null,
            string serverUrl = null, bool createNew = false)
        {
            var serverCertPath = GenerateAndSaveSelfSignedCertificate(createNew);

            if (customSettings == null)
                customSettings = new ConcurrentDictionary<string, string>();

            customSettings[RavenConfiguration.GetKey(x => x.Security.CertificatePath)] = serverCertPath;
            customSettings[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = serverUrl ?? "https://" + Environment.MachineName + ":0";

            DoNotReuseServer(customSettings);

            return serverCertPath;
        }

        public class Options
        {
            private readonly bool _frozen;

            private X509Certificate2 _clientCertificate;
            private X509Certificate2 _adminCertificate;
            private bool _createDatabase;
            private bool _deleteDatabaseOnDispose;
            private RavenServer _server;
            private int _replicationFactor;
            private bool _ignoreDisabledDatabase;
            private Action<DocumentStore> _modifyDocumentStore;
            private Action<DatabaseRecord> _modifyDatabaseRecord;
            private Func<string, string> _modifyDatabaseName;
            private string _path;

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

            private void AssertNotFrozen()
            {
                if (_frozen)
                    throw new InvalidOperationException("Options are frozen and cannot be changed.");
            }
        }

        protected void CreateSimpleData(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var entityA = new Entity{ Id = "entity/1", Name = "A" };
                var entityB = new Entity{ Id = "entity/2", Name = "B" };
                var entityC = new Entity{ Id = "entity/3", Name = "C" };

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
    }
}
