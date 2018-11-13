using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Embedded;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Raven.TestDriver
{
    public class RavenTestDriver : IDisposable
    {
        private static readonly EmbeddedServer TestServer = new EmbeddedServer();

        private static readonly Lazy<IDocumentStore> TestServerStore = new Lazy<IDocumentStore>(RunServer, LazyThreadSafetyMode.ExecutionAndPublication);

        private readonly ConcurrentDictionary<DocumentStore, object> _documentStores = new ConcurrentDictionary<DocumentStore, object>();

        private static int _index;
        private static ServerOptions _globalServerOptions;

        private static FileInfo _emptySettingsFile;

        private static FileInfo EmptySettingsFile
        {
            get
            {
                if (_emptySettingsFile == null)
                {
                    _emptySettingsFile = new FileInfo(Path.GetTempFileName());
                    File.WriteAllText(_emptySettingsFile.FullName, "{}");
                }

                return _emptySettingsFile;
            }
        }

        protected virtual string DatabaseDumpFilePath => null;

        protected virtual Stream DatabaseDumpFileStream => null;

        protected bool IsDisposed { get; private set; }

        public static void ConfigureServer(TestServerOptions options)
        {
            if (TestServerStore.IsValueCreated)
                throw new InvalidOperationException($"Cannot configure server after it was started. Please call '{nameof(ConfigureServer)}' method before any '{nameof(GetDocumentStore)}' is called.");

            _globalServerOptions = options;
        }

        protected IDocumentStore GetDocumentStore(GetDocumentStoreOptions options = null, [CallerMemberName] string database = null)
        {
            options = options ?? GetDocumentStoreOptions.Default;
            var name = database + "_" + Interlocked.Increment(ref _index);
            var documentStore = TestServerStore.Value;

            var createDatabaseOperation = new CreateDatabaseOperation(new DatabaseRecord(name));
            documentStore.Maintenance.Server.Send(createDatabaseOperation);

            var store = new DocumentStore
            {
                Urls = documentStore.Urls,
                Database = name
            };

            PreInitialize(store);

            store.Initialize();

            store.AfterDispose += (sender, args) =>
            {
                if (_documentStores.TryRemove(store, out _) == false)
                    return;

                try
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, hardDelete: true));
                }
                catch (DatabaseDoesNotExistException)
                {
                }
                catch (NoLeaderException)
                {
                }
            };

            AsyncHelpers.RunSync(() => ImportDatabaseAsync(store, name));

            SetupDatabase(store);

            if (options.WaitForIndexingTimeout.HasValue)
                WaitForIndexing(store, name, options.WaitForIndexingTimeout);

            _documentStores[store] = null;

            return store;
        }

        protected virtual void PreInitialize(IDocumentStore documentStore)
        {
        }

        protected virtual void SetupDatabase(IDocumentStore documentStore)
        {
        }

        protected event EventHandler DriverDisposed;

        protected void WaitForIndexing(IDocumentStore store, string database = null, TimeSpan? timeout = null)
        {
            var admin = store.Maintenance.ForDatabase(database);

            timeout = timeout ?? TimeSpan.FromMinutes(1);

            var sp = Stopwatch.StartNew();
            while (sp.Elapsed < timeout.Value)
            {
                var databaseStatistics = admin.Send(new GetStatisticsOperation());
                var indexes = databaseStatistics.Indexes
                    .Where(x => x.State != IndexState.Disabled);

                if (indexes.All(x => x.IsStale == false
                    && x.Name.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix) == false))
                    return;

                if (databaseStatistics.Indexes.Any(x => x.State == IndexState.Error))
                {
                    break;
                }

                Thread.Sleep(100);
            }

            var errors = admin.Send(new GetIndexErrorsOperation());

            string allIndexErrorsText = string.Empty;
            if (errors != null && errors.Length > 0)
            {
                var allIndexErrorsListText = string.Join("\r\n",
                    errors.Select(FormatIndexErrors));
                allIndexErrorsText = $"Indexing errors:\r\n{ allIndexErrorsListText }";

                string FormatIndexErrors(IndexErrors indexErrors)
                {
                    var errorsListText = string.Join("\r\n",
                        indexErrors.Errors.Select(x => $"- {x}"));
                    return $"Index '{indexErrors.Name}' ({indexErrors.Errors.Length} errors):\r\n{errorsListText}";
                }
            }

            throw new TimeoutException($"The indexes stayed stale for more than {timeout.Value}.{ allIndexErrorsText }");
        }

        protected void WaitForUserToContinueTheTest(IDocumentStore store)
        {
            var databaseNameEncoded = Uri.EscapeDataString(store.Database);
            var documentsPage = store.Urls[0] + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

            OpenBrowser(documentsPage); // start the server

            do
            {
                Thread.Sleep(500);

                using (var session = store.OpenSession())
                {
                    if (session.Advanced.Exists("Debug/Done"))
                    {
                        session.Delete("Debug/Done");
                        session.SaveChanges();
                        break;
                    }
                }
            } while (true);
        }

        protected virtual void OpenBrowser(string url)
        {
            Console.WriteLine(url);

            if (PlatformDetails.RunningOnPosix == false)
            {
                Process.Start(new ProcessStartInfo("cmd", $"/c start \"Stop & look at Studio\" \"{url}\""));
                return;
            }

            if (PlatformDetails.RunningOnMacOsx)
            {
                Process.Start("open", url);
                return;
            }

            Process.Start("xdg-open", url);
        }

        public virtual void Dispose()
        {
            if (IsDisposed)
                return;

            var exceptions = new List<Exception>();
            var stores = _documentStores.Keys.ToList();
            foreach (var documentStore in stores)
            {
                try
                {
                    documentStore.Dispose();
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }

            DatabaseDumpFileStream?.Dispose();

            IsDisposed = true;

            DriverDisposed?.Invoke(this, null);

            if (exceptions.Count > 0)
                throw new AggregateException(exceptions);
        }

        private async Task ImportDatabaseAsync(DocumentStore docStore, string database, TimeSpan? timeout = null)
        {
            var options = new DatabaseSmugglerImportOptions();
            if (DatabaseDumpFilePath != null)
            {
                var operation = await docStore.Smuggler.ForDatabase(database).ImportAsync(options, DatabaseDumpFilePath);
                await operation.WaitForCompletionAsync(timeout);
            }
            else if (DatabaseDumpFileStream != null)
            {
                var operation = await docStore.Smuggler.ForDatabase(database).ImportAsync(options, DatabaseDumpFileStream);
                await operation.WaitForCompletionAsync(timeout);
            }
        }

        private static IDocumentStore RunServer()
        {
            var options = _globalServerOptions ?? new TestServerOptions();
            options.CommandLineArgs.Insert(0, $"-c {CommandLineArgumentEscaper.EscapeSingleArg(EmptySettingsFile.FullName)}");
            options.CommandLineArgs.Add("--RunInMemory=true");

            TestServer.StartServer(options);

            var url = AsyncHelpers.RunSync(() => TestServer.GetServerUriAsync());

            var store = new DocumentStore
            {
                Urls = new[] { url.AbsoluteUri }
            };

            store.Initialize();

            return store;
        }
    }
}
