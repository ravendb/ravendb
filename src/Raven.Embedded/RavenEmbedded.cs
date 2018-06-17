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


namespace Raven.Embedded
{
    public class RavenEmbedded
    {
        private static Process _ravenDbSlavedProcess;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenEmbedded>("Embedded");
        private Task<Uri> _serverTask;
        private long _initialized;

        private readonly ConcurrentDictionary<string, IDocumentStore> _documentStores = new ConcurrentDictionary<string, IDocumentStore>();

        public void StartServer(ServerOptions options = null)
        {
            // Start the server

            if (Interlocked.CompareExchange(ref _initialized, 1, 0) != 0)
                throw new InvalidOperationException("Already have been initialized");

            options = options ?? ServerOptions.Default;
            _serverTask = RunServer(options);
        }

        public static RavenEmbedded Instance = new RavenEmbedded();

        public async Task<IDocumentStore> GetDocumentStore(string database)
        {
            if (Interlocked.Read(ref _initialized) == 0)
                throw new InvalidOperationException("Please run Setup before trying to get document store");

            if (string.IsNullOrEmpty(database))
                throw new ArgumentException(nameof(database));

            if (Logger.IsInfoEnabled)
                Logger.Info($"GetDocumentStore for db ${ database }.");

            var serverUrl = await GetServerUri();

            if (_documentStores.TryGetValue(database, out var existingStore))
            {
                return existingStore;
            }

            var store = new DocumentStore
            {
                Urls = new[] { serverUrl.AbsoluteUri },
                Database = database

            }.Initialize();

            try
            {
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database))).ConfigureAwait(false);
            }
            catch (ConcurrencyException)
            {
                // Expected behaviour when the database is already exists

                if (Logger.IsInfoEnabled)
                    Logger.Info($"{database} already exist.");
            }

            _documentStores.TryAdd(database, store);
            return store;

        }

        public async Task<Uri> GetServerUri()
        {
            if (Interlocked.Read(ref _initialized) == 0)
                throw new InvalidOperationException("Please run Setup before trying to get the server uri");

            return await _serverTask.ConfigureAwait(false);
        }

        public void RemoveStore(string database)
        {
            _documentStores.TryRemove(database, out _);
        }

        public void KillSlavedServerProcess()
        {
            var p = _ravenDbSlavedProcess;
            _ravenDbSlavedProcess = null;

            if (p == null || p.HasExited != false)
                return;

            if (Logger.IsInfoEnabled)
                Logger.Info($"Kill global server PID { p.Id }.");

            try
            {
                p.Kill();
                Interlocked.CompareExchange(ref _initialized, 0, 1);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"{DateTime.Now}: {e}\r\n");
                }

                if (p.HasExited)
                    Interlocked.CompareExchange(ref _initialized, 0, 1);

                throw;
            }
            finally
            {
                _documentStores.Clear();
            }
        }

        private static async Task<Uri> RunServer(ServerOptions options)
        {
            var process = _ravenDbSlavedProcess = RavenServerRunner.Run(options);
            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"Starting global server: { _ravenDbSlavedProcess.Id }");
            }

            string url = null;
            var output = process.StandardOutput;
            var sb = new StringBuilder();// listen to standard error as well

            var startupDuration = Stopwatch.StartNew();

            Task<string> readLineTask = null;
            while (true)
            {
                if (readLineTask == null)
                    readLineTask = output.ReadLineAsync();

                var hasResult = await readLineTask.WaitWithTimeout(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                if (startupDuration.Elapsed > TimeSpan.FromMinutes(1)) //from options
                    break;

                if (hasResult == false)
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
                        Logger.Info(e.Message);
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
                try
                {
                    process.Kill();
                }
                catch (Exception e)
                {
                    Logger.Info(e.Message);
                }

                throw new InvalidOperationException("Unable to start server, log is: " + Environment.NewLine + log);
            }

            return new Uri(url);
        }
    }

}
