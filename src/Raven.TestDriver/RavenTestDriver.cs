using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#if NETSTANDARD1_5
using System.Runtime.Loader;
#endif
using System.Text;
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

namespace Raven.TestDriver
{
    public class RavenTestDriver<TServerLocator> : IDisposable
        where TServerLocator : RavenServerLocator, new()
    {
        private static readonly Lazy<IDocumentStore> GlobalServer =
            new Lazy<IDocumentStore>(RunServer, LazyThreadSafetyMode.ExecutionAndPublication);

        private static Process _globalServerProcess;

        private readonly ConcurrentDictionary<DocumentStore, object> _documentStores =
            new ConcurrentDictionary<DocumentStore, object>();

        private static int _index;

        protected virtual string DatabaseDumpFilePath => null;

        protected virtual Stream DatabaseDumpFileStream => null;

        protected bool IsDisposed { get; private set; }

        public static bool Debug { get; set; }

        public static Process GlobalServerProcess => _globalServerProcess;

        public IDocumentStore GetDocumentStore(GetDocumentStoreOptions options = null, [CallerMemberName] string database = null)
        {
            options = options ?? GetDocumentStoreOptions.Default;
            var name = database + "_" + Interlocked.Increment(ref _index);
            ReportInfo($"GetDocumentStore for db ${ database }.");
            var documentStore = GlobalServer.Value;

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
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, true));
                }
                catch (DatabaseDoesNotExistException)
                {
                }
                catch (NoLeaderException)
                {
                }
            };

            ImportDatabase(store, name);

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

        private void ImportDatabase(DocumentStore docStore, string database)
        {
            var options = new DatabaseSmugglerImportOptions();
            if (DatabaseDumpFilePath != null)
            {
                AsyncHelpers.RunSync(() => docStore.Smuggler.ForDatabase(database)
                    .ImportAsync(options, DatabaseDumpFilePath));
            }
            else if (DatabaseDumpFileStream != null)
            {
                AsyncHelpers.RunSync(() => docStore.Smuggler.ForDatabase(database)
                    .ImportAsync(options, DatabaseDumpFileStream));
            }
        }

        private static IDocumentStore RunServer()
        {
            var process = _globalServerProcess = RavenServerRunner<TServerLocator>.Run(new TServerLocator());

            ReportInfo($"Starting global server: { _globalServerProcess.Id }");

#if NETSTANDARD1_3
            AppDomain.CurrentDomain.ProcessExit += (s, args) =>
            {
                KillGlobalServerProcess();
            };
#endif

#if NETSTANDARD1_5
            AssemblyLoadContext.Default.Unloading += c =>
            {
                KillGlobalServerProcess();
            };
#endif

            string url = null;
            var output = process.StandardOutput;
            var sb = new StringBuilder();

            var startupDuration = Stopwatch.StartNew();

            Task<string> readLineTask = null;
            while (true)
            {
                if (readLineTask == null)
                    readLineTask = output.ReadLineAsync();

                var task = Task.WhenAny(readLineTask, Task.Delay(TimeSpan.FromSeconds(5))).Result;

                if (startupDuration.Elapsed > TimeSpan.FromMinutes(1))
                    break;

                if (task != readLineTask)
                    continue;

                var line = readLineTask.Result;

                readLineTask = null;

                sb.AppendLine(line);

                if (line == null)
                {
                    try
                    {
                        process.Kill();
                    }
                    catch (Exception e)
                    {
                        ReportError(e);
                    }

                    throw new InvalidOperationException("Unable to start server, log is: " + Environment.NewLine + sb);
                }
                const string prefix = "Server available on: ";
                if (line.StartsWith(prefix))
                {
                    url = line.Substring(prefix.Length);
                    break;
                }
            }

            if (url == null)
            {
                var log = sb.ToString();
                ReportInfo(log);
                try
                {
                    process.Kill();
                }
                catch (Exception e)
                {
                    ReportError(e);
                }

                throw new InvalidOperationException("Unable to start server, log is: " + Environment.NewLine + log);
            }

            output.ReadToEndAsync()
                .ContinueWith(x =>
                {
                    ReportError(x.Exception);
                    GC.KeepAlive(x.Exception);
                }); // just discard any other output

            var store = new DocumentStore
            {
                Urls = new[] { url },
                Database = "test.manager"
            };

            return store.Initialize();
        }

        private static void KillGlobalServerProcess()
        {
            var p = _globalServerProcess;
            _globalServerProcess = null;
            if (p != null && p.HasExited == false)
            {
                ReportInfo($"Kill global server PID { p.Id }.");

                try
                {
                    p.Kill();
                }
                catch (Exception e)
                {
                    ReportError(e);
                }
            }

            if (File.Exists(RavenServerRunner<TServerLocator>.EmptySettingsFilePath))
            {
                File.Delete(RavenServerRunner<TServerLocator>.EmptySettingsFilePath);
            }
        }

        public void WaitForIndexing(IDocumentStore store, string database = null, TimeSpan? timeout = null)
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

        public void WaitForUserToContinueTheTest(IDocumentStore store)
        {
            var databaseNameEncoded = Uri.EscapeDataString(store.Database);
            var documentsPage = store.Urls[0] + "/studio/index.html#databases/documents?&database=" + databaseNameEncoded + "&withStop=true";

            OpenBrowser(documentsPage); // start the server

            do
            {
                Thread.Sleep(500);

                using (var session = store.OpenSession())
                {
                    if (session.Load<object>("Debug/Done") != null)
                        break;
                }
            } while (true);
        }

        protected virtual void OpenBrowser(string url)
        {
            Console.WriteLine(url);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Process.Start(new ProcessStartInfo("cmd", $"/c start \"Stop & look at studio\" \"{url}\"")); // Works ok on windows
                return;
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Process.Start("xdg-open", url); // Works ok on linux
                return;
            }

            throw new NotImplementedException("Implement your own browser opening mechanism.");
        }

        private static void ReportError(Exception e)
        {
            if (Debug == false)
                return;

            if (e == null)
                throw new ArgumentNullException(nameof(e));

            var msg = $"{DateTime.Now}: {e}\r\n";
            try
            {
                File.AppendAllText("raven_testdriver.log", msg);
            }
            catch (Exception)
            {
            }
            Console.WriteLine(msg);
        }

        private static void ReportInfo(string message)
        {
            if (Debug == false)
                return;

            if (string.IsNullOrWhiteSpace(message))
                throw new ArgumentNullException(nameof(message));

            var msg = $"{DateTime.Now}: {message}\r\n";
            try
            {
                File.AppendAllText("raven_testdriver.log", msg);

            }
            catch (Exception)
            {
            }
            Console.WriteLine(msg);
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
    }
}
