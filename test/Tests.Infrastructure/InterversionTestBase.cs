using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Utils;
using Raven.TestDriver;
using Tests.Infrastructure.InterversionTest;

namespace Tests.Infrastructure
{
    public abstract class InterversionTestBase : RavenTestBase
    {
        private static readonly ConcurrentBag<Process> _allLaunchedServerProcesses = new ConcurrentBag<Process>();

        private readonly HashSet<Process> _testInstanceServerProcesses = new HashSet<Process>();

        private static readonly ServerBuildRetriever _serverBuildRetriever = new ServerBuildRetriever();

        protected DocumentStore GetDocumentStore(
            string serverVersion,
            InterversionTestOptions options = null,
            [CallerMemberName] string database = null)
        {
            return AsyncHelpers.RunSync(() => GetDocumentStoreAsync(serverVersion, options, database));
        }

        protected async Task<DocumentStore> GetDocumentStoreAsync(
            string serverVersion,
            InterversionTestOptions options = null,
            [CallerMemberName] string database = null,
            CancellationToken token = default)
        {
            var (serverUrl, serverProcess) = await GetServerAsync(serverVersion, options, database, token);

            options = options ?? InterversionTestOptions.Default;
            var name = GetDatabaseName(database);

            if (options.ModifyDatabaseName != null)
                name = options.ModifyDatabaseName(name) ?? name;

            var doc = new DatabaseRecord(name)
            {
                Settings =
                        {
                            [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "1",
                            [RavenConfiguration.GetKey(x => x.Replication.RetryReplicateAfter)] = "1",
                            [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "true",
                            [RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "true",
                            [RavenConfiguration.GetKey(x => x.Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory)] = int.MaxValue.ToString()
                        }
            };

            options.ModifyDatabaseRecord?.Invoke(doc);

            var store = new DocumentStore
            {
                Urls = new[] { serverUrl },
                Database = name
            };

            options.ModifyDocumentStore?.Invoke(store);

            store.Initialize();

            if (options.CreateDatabase)
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc, options.ReplicationFactor), token);

            store.AfterDispose += (sender, e) =>
            {
                KillSlavedServerProcess(serverProcess);
            };

            return store;
        }


        protected async Task<(string ServerUrl, Process ServerProcess)> GetServerAsync(
            string serverVersion,
            InterversionTestOptions options = null,
            [CallerMemberName] string database = null,
            CancellationToken token = default)
        {
            var serverBuildInfo = ServerBuildDownloadInfo.Create(serverVersion);
            var serverPath = await _serverBuildRetriever.GetServerPath(serverBuildInfo, token);
            var testServerPath = NewDataPath(prefix: serverVersion);
            CopyFilesRecursively(new DirectoryInfo(serverPath), new DirectoryInfo(testServerPath));

            var locator = new ConfigurableRavenServerLocator(testServerPath);
            return await RunServer(locator);
        }

        public static void CopyFilesRecursively(DirectoryInfo source, DirectoryInfo target)
        {
            foreach (DirectoryInfo dir in source.GetDirectories())
                CopyFilesRecursively(dir, target.CreateSubdirectory(dir.Name));
            foreach (FileInfo file in source.GetFiles())
                file.CopyTo(Path.Combine(target.FullName, file.Name));
        }

        private async Task<(string ServerUrl, Process ServerProcess)> RunServer(ConfigurableRavenServerLocator locator)
        {
            var process = RunServerProcess(locator);

            string url = null;
            var startupDuration = Stopwatch.StartNew();

            var outputString = await ReadOutput(process.StandardOutput, startupDuration, async (line, builder) =>
            {
                if (line == null)
                {
                    var errorString = await ReadOutput(process.StandardError, startupDuration, null).ConfigureAwait(false);

                    KillSlavedServerProcess(process);

                    throw new InvalidOperationException(BuildStartupExceptionMessage(builder.ToString(), errorString));
                }

                const string prefix = "Server available on: ";
                if (line.StartsWith(prefix))
                {
                    url = line.Substring(prefix.Length);
                    return true;
                }

                return false;
            }).ConfigureAwait(false);

            if (url == null)
            {
                var errorString = await ReadOutput(process.StandardError, startupDuration, null).ConfigureAwait(false);

                KillSlavedServerProcess(process);

                throw new InvalidOperationException(BuildStartupExceptionMessage(outputString, errorString));
            }

            return (url, process);
        }

        private static string BuildStartupExceptionMessage(string outputString, string errorString)
        {
            var sb = new StringBuilder();
            sb.AppendLine("Unable to start the RavenDB Server");

            if (string.IsNullOrWhiteSpace(errorString) == false)
            {
                sb.AppendLine("Error:");
                sb.AppendLine(errorString);
            }

            if (string.IsNullOrWhiteSpace(outputString) == false)
            {
                sb.AppendLine("Output:");
                sb.AppendLine(outputString);
            }

            return sb.ToString();
        }

        protected static void KillSlavedServerProcess(Process process)
        {
            if (process == null || process.HasExited)
                return;

            try
            {
                process.Kill();
            }
            catch (Exception e)
            {
                ReportError(e);
            }
        }

        private static async Task<string> ReadOutput(StreamReader output, Stopwatch startupDuration, Func<string, StringBuilder, Task<bool>> onLine)
        {
            var sb = new StringBuilder();

            Task<string> readLineTask = null;
            while (true)
            {
                if (readLineTask == null)
                    readLineTask = output.ReadLineAsync();

                var hasResult = await readLineTask.WaitWithTimeout(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

                //if (startupDuration.Elapsed > options.MaxServerStartupTimeDuration)
                //    return null;

                if (hasResult == false)
                    continue;

                var line = readLineTask.Result;

                readLineTask = null;

                if (line != null)
                    sb.AppendLine(line);

                var shouldStop = false;
                if (onLine != null)
                    shouldStop = await onLine(line, sb).ConfigureAwait(false);

                if (shouldStop)
                    break;

                if (line == null)
                    break;
            }

            return sb.ToString();
        }

        private Process RunServerProcess(ConfigurableRavenServerLocator locator)
        {
            var process = RavenServerRunner<ConfigurableRavenServerLocator>.Run(locator);

            _allLaunchedServerProcesses.Add(process);
            _testInstanceServerProcesses.Add(process);
            return process;
        }

        private static void ReportError(Exception e)
        {
            if (e == null)
                throw new ArgumentNullException(nameof(e));

            var msg = $"{DateTime.Now}: {e}\r\n";
            Console.WriteLine(msg);
        }

        protected override void Dispose(ExceptionAggregator exceptionAggregator)
        {
            foreach (var serverProcess in _testInstanceServerProcesses)
            {
                exceptionAggregator.Execute(() =>
                {
                    if (serverProcess.HasExited == false)
                        serverProcess.Kill();
                });
            }
        }

        protected async Task<bool> WaitForDocumentInClusterAsync<T>(string docId, Func<T, bool> predicate, TimeSpan timeout, List<DocumentStore> stores, string database = null)
        {
            var tasks = new List<Task<bool>>();

            foreach (var store in stores)
                tasks.Add(Task.Run(() => WaitForDocument(store, docId, predicate, (int)timeout.TotalMilliseconds, database)));

            await Task.WhenAll(tasks);

            return tasks.All(x => x.Result);
        }


        protected bool WaitForDocument<T>(IDocumentStore store,
            string docId,
            Func<T, bool> predicate,
            int timeout = 10000,
            string database = null)
        {
            if (DebuggerAttachedTimeout.DisableLongTimespan == false &&
                Debugger.IsAttached)
                timeout *= 100;

            database = database ?? store.Database;
            var sw = Stopwatch.StartNew();
            Exception ex = null;
            while (sw.ElapsedMilliseconds < timeout)
            {
                using (var session = store.OpenSession(database))
                {
                    try
                    {
                        var doc = session.Load<T>(docId);
                        if (doc != null)
                        {
                            if (predicate == null || predicate(doc))
                                return true;
                        }
                    }
                    catch (Exception e)
                    {
                        ex = e;
                        // expected that we might get conflict, ignore and wait
                    }
                }

                Thread.Sleep(100);
            }

            using (var session = store.OpenSession(database))
            {
                //one last try, and throw if there is still a conflict
                var doc = session.Load<T>(docId);
                if (doc != null)
                {
                    if (predicate == null || predicate(doc))
                        return true;
                }
            }
            if (ex != null)
            {
                throw ex;
            }
            return false;
        }

        public class InterversionTestOptions
        {
            private readonly bool _frozen;

            private bool _createDatabase;
            private int _replicationFactor;
            private Action<DocumentStore> _modifyDocumentStore;
            private Action<DatabaseRecord> _modifyDatabaseRecord;
            private Func<string, string> _modifyDatabaseName;

            public static readonly InterversionTestOptions Default = new InterversionTestOptions(true);

            public InterversionTestOptions() : this(false)
            {
            }

            private InterversionTestOptions(bool frozen)
            {
                CreateDatabase = true;
                ReplicationFactor = 1;

                _frozen = frozen;
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

            public int ReplicationFactor
            {
                get => _replicationFactor;
                set
                {
                    AssertNotFrozen();
                    _replicationFactor = value;
                }
            }

            public bool CreateDatabase
            {
                get => _createDatabase;
                set
                {
                    AssertNotFrozen();
                    _createDatabase = value;
                }
            }

            private void AssertNotFrozen()
            {
                if (_frozen)
                    throw new InvalidOperationException("Options are frozen and cannot be changed.");
            }
        }
    }


}
