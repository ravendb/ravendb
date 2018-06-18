using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Logging;
using Raven.Client.Util;

namespace Raven.Embedded
{
    public class EmbeddedServer :  IDisposable
    {
        public static EmbeddedServer Instance = new EmbeddedServer();

        private readonly Logger Logger = LoggingSource.Instance.GetLogger<EmbeddedServer>("Embedded");
        private Lazy<Task<(Uri ServerUrl, Process ServerProcess)>> _serverTask;
        private long _initialized;

        private readonly ConcurrentDictionary<string, Lazy<Task<IDocumentStore>>> _documentStores = new ConcurrentDictionary<string, Lazy<Task<IDocumentStore>>>();

        public void StartServer(ServerOptions options = null)
        {
            options = options ?? ServerOptions.Default;

            var startServer = new Lazy<Task<(Uri ServerUrl, Process ServerProcess)>>(() => RunServer(options));
            if (Interlocked.CompareExchange(ref _serverTask, startServer, null) != null)
                throw new InvalidOperationException("The server was already started");

            // this forces the server to start running in an async manner.
            GC.KeepAlive(startServer.Value);
        }


        public IDocumentStore GetDocumentStore(string database)
        {
            return AsyncHelpers.RunSync(() => GetDocumentStoreAsync(database));
        }

        public IDocumentStore GetDocumentStore(GetDocumentStoreOptions options)
        {
            return AsyncHelpers.RunSync(() => GetDocumentStoreAsync(options));
        }

        public Task<IDocumentStore> GetDocumentStoreAsync(string database)
        {
            return GetDocumentStoreAsync(new GetDocumentStoreOptions
            {
                DatabaseName = database
            });
        }

        public async Task<IDocumentStore> GetDocumentStoreAsync(GetDocumentStoreOptions options)
        {
            var db = options.DatabaseName;
            if (string.IsNullOrEmpty(db))
                throw new ArgumentException("The database name is mandatory");

            if (Logger.IsInfoEnabled)
                Logger.Info($"Creating document store for ${db}.");

            var lazy = new Lazy<Task<IDocumentStore>>(async () =>
            {
                var serverUrl = await GetServerUriAsync();
                var store = new DocumentStore
                {
                    Urls = new[] { serverUrl.AbsoluteUri },
                    Database = db

                };
                store.AfterDispose += (sender, args) => _documentStores.TryRemove(db, out _);

                store.Initialize();
                if (options.SkipCreatingDatabase == false)
                    await TryCreateDatabase(options, store);

                return store;
            });

            return await _documentStores.GetOrAdd(db, lazy).Value;

        }

        private async Task TryCreateDatabase(GetDocumentStoreOptions options, IDocumentStore store)
        {
            try
            {
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(options.DatabaseName))).ConfigureAwait(false);
            }
            catch (ConcurrencyException)
            {
                // Expected behaviour when the database is already exists

                if (Logger.IsInfoEnabled)
                    Logger.Info($"{options.DatabaseName} already exist.");
            }
        }

        public async Task<Uri> GetServerUriAsync()
        {
            var server = _serverTask;
            if (server == null)
                throw new InvalidOperationException("Please run StartServer() before trying to use the server");

            return (await server.Value.ConfigureAwait(false)).ServerUrl;
        }

        private void KillSlavedServerProcess(Process process)
        {
            if (process == null || process.HasExited != false)
                return;

            if (Logger.IsInfoEnabled)
                Logger.Info($"Killing global server PID { process.Id }.");

            try
            {
                process.Kill();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Failed to kill process {process.Id}", e);
                }
            }
        }

        private async Task<(Uri ServerUrl, Process ServerProcess)> RunServer(ServerOptions options)
        {
            var process =  RavenServerRunner.Run(options);
            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"Starting global server: { process.Id }");
            }

            string url = null;
            var output = process.StandardOutput;
            var sb = new StringBuilder();//TODO: listen to standard error as well

            var startupDuration = Stopwatch.StartNew();

            Task<string> readLineTask = null;
            while (true)
            {
                if (readLineTask == null)
                    readLineTask = output.ReadLineAsync();

                var hasResult = await readLineTask.WaitWithTimeout(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                if (startupDuration.Elapsed > options.MaxServerStartupTimeDuration) 
                    break;

                if (hasResult == false)
                    continue;

                var line = readLineTask.Result;

                readLineTask = null;

                sb.AppendLine(line);

                if (line == null)
                {
                    KillSlavedServerProcess(process);

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

                KillSlavedServerProcess(process);

                throw new InvalidOperationException("Unable to start server, log is: " + Environment.NewLine + log);
            }

            return (new Uri(url), process);
        }

        public void Dispose()
        {
            var lazy = Interlocked.Exchange(ref _serverTask, null);
            if (lazy == null || lazy.IsValueCreated == false)
                return;

            var process = lazy.Value.Result.ServerProcess;
            KillSlavedServerProcess(process);
            foreach (var item in _documentStores)
            {
                if (item.Value.IsValueCreated)
                {
                    item.Value.Value.Dispose();
                }
            }
            _documentStores.Clear();
        }
    }
}
