using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Extensions;
using Raven.Server.Utils;
using Raven.TestDriver;
using Tests.Infrastructure.InterversionTest;

namespace Tests.Infrastructure
{
    public abstract class InterversionTestBase : RavenTestBase
    {
        private static ConcurrentBag<Process> _allLaunchedServerProcesses = new ConcurrentBag<Process>();

        private HashSet<Process> _testInstanceServerProcesses = new HashSet<Process>();

        private static ServerBuildRetriever _serverBuildRetriever = new ServerBuildRetriever();

        protected async Task<IDocumentStore> GetDocumentStoreAsync()
        {
            return GetDocumentStore();
        }

        protected async Task<IDocumentStore> GetDocumentStoreAsync(
            string serverVersion, [CallerMemberName] string database = null, CancellationToken token = default(CancellationToken))
        {
            var serverBuildInfo = ServerBuildDownloadInfo.Create(serverVersion);
            var serverPath = await _serverBuildRetriever.GetServerPath(serverBuildInfo);
            var testServerPath = NewDataPath(prefix: serverVersion);
            CopyFilesRecursively(new DirectoryInfo(serverPath), new DirectoryInfo(testServerPath));
            var locator = new ConfigurableRavenServerLocator(testServerPath);
            var (serverUrl, serverProcess) = await RunServer(locator);
            return new DocumentStore() {
                Urls = new [] { serverUrl.ToString() },
                Database = database
            };
        }

        public static void CopyFilesRecursively(DirectoryInfo source, DirectoryInfo target)
        {
            foreach (DirectoryInfo dir in source.GetDirectories())
                CopyFilesRecursively(dir, target.CreateSubdirectory(dir.Name));
            foreach (FileInfo file in source.GetFiles())
                file.CopyTo(Path.Combine(target.FullName, file.Name));
        }

        private async Task<(Uri ServerUrl, Process ServerProcess)> RunServer(ConfigurableRavenServerLocator locator)
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

            return (new Uri(url), process);
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

        private void KillSlavedServerProcess(Process process)
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
    }
}
