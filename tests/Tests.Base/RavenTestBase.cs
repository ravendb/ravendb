using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Loader;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Data.Indexes;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Json.Parsing;
using Sparrow.Json;


//using JsonTextWriter = Raven.Imports.Newtonsoft.Json.JsonTextWriter;

namespace NewClientTests
{
    public class RavenTestBase : LinuxRaceConditionWorkAround, IDisposable
    {
        public const string ServerName = "Raven.Tests.Core.Server";

        protected readonly ConcurrentSet<DocumentStore> CreatedStores = new ConcurrentSet<DocumentStore>();

        protected static readonly ConcurrentSet<string> PathsToDelete =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private static RavenServer _globalServer;
        private static readonly object ServerLocker = new object();
        private static readonly object AvailableServerPortsLocker = new object();
        private RavenServer _localServer;

        public void DoNotReuseServer() => _doNotReuseServer = true;
        private bool _doNotReuseServer;
        private int NonReusedServerPort { get; set; }
        private int NonReusedTcpServerPort { get; set; }
        private const int MaxParallelServer = 79;
        private static readonly List<int> _usedServerPorts = new List<int>();

        private static readonly List<int> _availableServerPorts =
            Enumerable.Range(8079 - MaxParallelServer, MaxParallelServer).ToList();

        public async Task<DocumentDatabase> GetDatabase(string databaseName)
        {
            var database = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            return database;
        }

        public RavenServer Server
        {
            get
            {
                if (_localServer != null)
                    return _localServer;

                if (_doNotReuseServer)
                {
                    NonReusedServerPort = GetAvailablePort();
                    NonReusedTcpServerPort = GetAvailablePort();
                    _localServer = CreateServer(NonReusedServerPort, NonReusedTcpServerPort);
                    return _localServer;
                }

                if (_globalServer != null)
                {
                    _localServer = _globalServer;
                    return _localServer;
                }
                lock (ServerLocker)
                {
                    if (_globalServer == null)
                    {
                        Console.WriteLine("\tTo attach debugger to test process, use process id: {0}", Process.GetCurrentProcess().Id);
                        var globalServer = CreateServer(GetAvailablePort(), GetAvailablePort());
                        AssemblyLoadContext.Default.Unloading += context =>
                        {
                            globalServer.Dispose();

                            GC.Collect(2);
                            GC.WaitForPendingFinalizers();

                            var exceptionAggregator = new ExceptionAggregator("Failed to cleanup test databases");

                            RavenTestHelper.DeletePaths(PathsToDelete, exceptionAggregator);

                            exceptionAggregator.ThrowIfNeeded();
                        };
                        _globalServer = globalServer;
                    }
                    _localServer = _globalServer;
                }
                return _globalServer;
            }
        }

        private static int GetAvailablePort()
        {
            int available;
            lock (AvailableServerPortsLocker)
            {
                if (_availableServerPorts.Count != 0)
                {
                    available = _availableServerPorts[0];
                    _usedServerPorts.Add(available);
                    _availableServerPorts.RemoveAt(0);
                }
                else
                {
                    throw new InvalidOperationException(
                        $"Maximum allowed parallel servers pool in test is exhausted (max={MaxParallelServer}");
                }
            }
            return available;
        }

        private static void RemoveUsedPort(int port)
        {
            lock (AvailableServerPortsLocker)
            {
                _availableServerPorts.Add(port);
                _usedServerPorts.Remove(port);
            }
        }

        private static RavenServer CreateServer(int port, int tcpPort)
        {
            var configuration = new RavenConfiguration();
            configuration.Initialize();
            configuration.DebugLog.LogMode = LogMode.None;
            configuration.Core.ServerUrl = $"http://localhost:{port}";
            configuration.Core.TcpServerUrl = $"tcp://localhost:{tcpPort}";
            configuration.Server.Name = ServerName;
            configuration.Core.RunInMemory = true;
            string postfix = port == 8080 ? "" : "_" + port;
            configuration.Core.DataDirectory = Path.Combine(configuration.Core.DataDirectory, $"Tests{postfix}");
            configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad = new TimeSetting(10, TimeUnit.Seconds);
            configuration.Storage.AllowOn32Bits = true;

            IOExtensions.DeleteDirectory(configuration.Core.DataDirectory);

            var server = new RavenServer(configuration);
            server.Initialize();

            // TODO: Make sure to properly handle this when this is resolved:
            // TODO: https://github.com/dotnet/corefx/issues/5205
            // TODO: AssemblyLoadContext.GetLoadContext(typeof(RavenTestBase).GetTypeInfo().Assembly).Unloading +=

            return server;
        }

        protected Task<DocumentDatabase> GetDocumentDatabaseInstanceFor(DocumentStore store)
        {
            return Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.DefaultDatabase);
        }

        private static int _counter;

        protected virtual DocumentStore GetDocumentStore([CallerMemberName] string caller = null,
            string dbSuffixIdentifier = null, string path = null,
            Action<DatabaseDocument> modifyDatabaseDocument = null, string apiKey = null)
        {
            var name = caller != null ? $"{caller}_{Interlocked.Increment(ref _counter)}" : Guid.NewGuid().ToString("N");

            if (dbSuffixIdentifier != null)
                name = $"{name}_{dbSuffixIdentifier}";

            var hardDelete = true;
            var runInMemory = true;

            if (path == null)
                path = NewDataPath(name);
            else
            {
                hardDelete = false;
                runInMemory = false;
            }

            var doc = MultiDatabase.CreateDatabaseDocument(name);
            doc.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = runInMemory.ToString();
            doc.Settings[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] = path;
            doc.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexOrTransformerCouldNotBeOpened)] = "true";
            doc.Settings[RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString();
            modifyDatabaseDocument?.Invoke(doc);

            TransactionOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                if (Server.ServerStore.Read(context, Constants.Database.Prefix + name) != null)
                    throw new InvalidOperationException($"Database '{name}' already exists");
            }

            var store = new DocumentStore
            {
                Url = UseFiddler(Server.Configuration.Core.ServerUrl),
                DefaultDatabase = name,
                ApiKey = apiKey
            };
            ModifyStore(store);
            store.Initialize();

            store.DatabaseCommands.GlobalAdmin.CreateDatabase(doc);
            store.AfterDispose += (sender, args) =>
            {
                var databaseTask = Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(name);
                if (databaseTask != null && databaseTask.IsCompleted == false)
                    databaseTask.Wait(); // if we are disposing store before database had chance to load then we need to wait

                store.DatabaseCommands.GlobalAdmin.DeleteDatabase(name, hardDelete: hardDelete);
                CreatedStores.TryRemove(store);
            };
            CreatedStores.Add(store);
            return store;
        }

        protected virtual void ModifyStore(DocumentStore store)
        {

        }

        private static string UseFiddler(string url)
        {
            if (Debugger.IsAttached && Process.GetProcessesByName("fiddler").Any())
                return url.Replace("localhost", "localhost.fiddler");

            return url;
        }

        public static void WaitForIndexing(IDocumentStore store, string database = null, TimeSpan? timeout = null)
        {
            var databaseCommands = store.DatabaseCommands;
            if (database != null)
            {
                databaseCommands = databaseCommands.ForDatabase(database);
            }

            timeout = timeout ?? (Debugger.IsAttached
                          ? TimeSpan.FromMinutes(15)
                          : TimeSpan.FromMinutes(1));


            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var databaseStatistics = databaseCommands.GetStatistics();
                if (databaseStatistics.Indexes.All(x => x.IsStale == false))
                    return;

                if (databaseStatistics.Indexes.Any(x => x.State == IndexState.Error))
                {
                    break;
                }
                Thread.Sleep(32);
            }

            var request = databaseCommands.CreateRequest("/indexes/performance", HttpMethod.Get);
            var perf = request.ReadResponseJson();
            request = databaseCommands.CreateRequest("/indexes/errors", HttpMethod.Get);
            var errors = request.ReadResponseJson();

            var total = new JObject
            {
                ["Errors"] = JObject.Parse(errors.ToString()),
                ["Performance"] = JObject.Parse(perf.ToString())
            };

            //var total = new RavenJObject
            //{
            //    ["Errors"] = errors,
            //    ["Performance"] = perf
            //};

            var file = Path.GetTempFileName() + ".json";
            using (var writer = File.CreateText(file))
            {
                var jsonTextWriter = new JsonTextWriter(writer);
                total.WriteTo(jsonTextWriter);
                jsonTextWriter.Flush();
            }

            var stats = databaseCommands.GetStatistics();

            var corrupted = stats.Indexes.Where(x => x.State == IndexState.Error).ToList();
            if (corrupted.Count > 0)
            {
                throw new InvalidOperationException(
                    $"The following indexes are with error state: {string.Join(",", corrupted.Select(x => x.Name))} - details at " + file);
            }

            throw new TimeoutException("The indexes stayed stale for more than " + timeout.Value + ", stats at " + file);
        }

        public static void WaitForUserToContinueTheTest(DocumentStore documentStore, bool debug = true, int port = 8079)
        {
            if (debug && Debugger.IsAttached == false)
                return;

            string url = documentStore.Url;

            var databaseNameEncoded = Uri.EscapeDataString(documentStore.DefaultDatabase);
            var documentsPage = url + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded +
                                "&withStop=true";

            OpenBrowser(documentsPage);// start the server

            do
            {
                Thread.Sleep(100);
            } while (documentStore.DatabaseCommands.Head("Debug/Done") == null && (debug == false || Debugger.IsAttached));
        }

        public static void OpenBrowser(string url)
        {
            Console.WriteLine(url);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Process.Start(new ProcessStartInfo("cmd", $"/c start \"Stop & look at studio\" \"{url}\"")); // Works ok on windows
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Process.Start("xdg-open", url); // Works ok on linux
            }
            else
            {
                Console.WriteLine("Do it yourself!");
            }
        }

        protected string NewDataPath([CallerMemberName] string prefix = null, string suffix = null,
            bool forceCreateDir = false)
        {
            if (suffix != null)
                prefix += suffix;
            var path = RavenTestHelper.NewDataPath(prefix, NonReusedServerPort, forceCreateDir);

            PathsToDelete.Add(path);
            return path;
        }

        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);

            var exceptionAggregator = new ExceptionAggregator("Could not dispose test");

            foreach (var store in CreatedStores)
                exceptionAggregator.Execute(store.Dispose);
            CreatedStores.Clear();

            if (_localServer != null)
            {
                if (_doNotReuseServer)
                {
                    exceptionAggregator.Execute(() =>
                    {
                        _localServer.Dispose();
                        _localServer = null;
                        RemoveUsedPort(NonReusedServerPort);
                        RemoveUsedPort(NonReusedTcpServerPort);
                    });
                }

                exceptionAggregator.ThrowIfNeeded();
            }
        }


        /// <summary>
        /// Get command for new client tests
        /// </summary>
        /// <param name="session"></param>
        /// <param name="ids"></param>
        /// <param name="etag"></param>
        /// <param name="documentInfo"></param>
        /// <returns></returns>
        public static BlittableJsonReaderObject GetCommand(DocumentSession session, string[] ids, 
            out DocumentInfo documentInfo)
        {
            var command = new GetDocumentCommand
            {
                Ids = ids
            };
            session.RequestExecuter.Execute(command, session.Context);
            var document = (BlittableJsonReaderObject)command.Result.Results[0];
            BlittableJsonReaderObject metadata;
            if (document.TryGet(Constants.Metadata.Key, out metadata) == false)
                throw new InvalidOperationException("Document must have a metadata");
            string id;
            if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                throw new InvalidOperationException("Document must have an id");
            long? etag;
            if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                throw new InvalidOperationException("Document must have an etag");
            documentInfo = new DocumentInfo
            {
                Id = id,
                Document = document,
                Metadata = metadata,
                ETag = etag
            };
            return document;
        }

        /// <summary>
        /// Put command for new client tests
        /// </summary>
        /// <param name="session"></param>
        /// <param name="entity"></param>
        /// <param name="id"></param>
        public void PutCommand(DocumentSession session, object entity, string id)
        {
            var documentInfo = new DocumentInfo
            {
                Entity = entity,
                Id = id
            };
            var tag = session.DocumentStore.Conventions.GetDynamicTagName(entity);

            var metadata = new DynamicJsonValue();
            if (tag != null)
                metadata[Constants.Headers.RavenEntityName] = tag;

            documentInfo.Metadata = session.Context.ReadObject(metadata, id);

            documentInfo.Document = session.EntityToBlittable.ConvertEntityToBlittable(documentInfo.Entity, documentInfo);

            var putCommand = new PutDocumentCommand()
            {
                Id = id,
                Etag = documentInfo.ETag,
                Document = documentInfo.Document,
                Context = session.Context
            };
            session.RequestExecuter.Execute(putCommand, session.Context);
        }
    }
}