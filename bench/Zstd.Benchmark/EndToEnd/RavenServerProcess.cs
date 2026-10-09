using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Zstd.Benchmark.EndToEnd
{
    /// <summary>
    /// A RavenDB server running in its own process (dotnet Raven.Server.dll), so its CPU time and memory can be measured
    /// without the benchmark client in the numbers. Stopped gracefully through the interactive console.
    /// </summary>
    internal sealed class RavenServerProcess : IAsyncDisposable
    {
        private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(30) };

        private readonly Process _process;
        private readonly StreamWriter _log;
        private readonly IntPtr? _affinity;

        private RavenServerProcess(Process process, string url, StreamWriter log, IntPtr? affinity)
        {
            _process = process;
            Url = url;
            _log = log;
            _affinity = affinity;
        }

        public string Url { get; }

        public int ProcessId => _process.Id;

        public int AffinityResets { get; private set; }

        public static async Task<RavenServerProcess> StartAsync(string serverPath, string dataDirectory, string logPath, int port,
            IReadOnlyDictionary<string, string> settings, IntPtr? affinity)
        {
            string url = $"http://127.0.0.1:{port}";
            ProcessStartInfo startInfo = new("dotnet")
            {
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                WorkingDirectory = Path.GetDirectoryName(serverPath)
            };
            startInfo.ArgumentList.Add(serverPath);
            startInfo.ArgumentList.Add($"--ServerUrl={url}");
            startInfo.ArgumentList.Add("--Setup.Mode=None");
            startInfo.ArgumentList.Add("--License.Eula.Accepted=true");
            startInfo.ArgumentList.Add($"--DataDir={dataDirectory}");
            startInfo.ArgumentList.Add($"--Logs.Path={Path.Combine(Path.GetDirectoryName(logPath), "logs")}");
            startInfo.ArgumentList.Add("--Logs.MinLevel=Warn");
            foreach ((string key, string value) in settings)
                startInfo.ArgumentList.Add($"--{key}={value}");

            StreamWriter log = new(logPath, append: true) { AutoFlush = true };
            log.WriteLine($"==== {DateTime.Now:O} dotnet {string.Join(' ', startInfo.ArgumentList)}");

            Process process = Process.Start(startInfo) ?? throw new InvalidOperationException("Could not start the server");
            process.OutputDataReceived += (_, e) => WriteLog(log, e.Data);
            process.ErrorDataReceived += (_, e) => WriteLog(log, e.Data);
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            RavenServerProcess server = new(process, url, log, affinity);
            try
            {
                await server.WaitUntilReadyAsync(TimeSpan.FromMinutes(3));
                server.EnsureAffinity();
                return server;
            }
            catch
            {
                await server.DisposeAsync();
                throw;
            }
        }

        /// <summary>
        /// The license manager sets the process affinity when the license is activated, so it is checked before every measurement.
        /// </summary>
        public void EnsureAffinity()
        {
            if (_affinity == null || OperatingSystem.IsWindows() == false)
                return;

            _process.Refresh();
            if (_process.ProcessorAffinity == _affinity.Value)
                return;

            _process.ProcessorAffinity = _affinity.Value;
            AffinityResets++;
        }

        public (TimeSpan Cpu, long PrivateBytes) Snapshot()
        {
            _process.Refresh();
            return (_process.TotalProcessorTime, _process.PrivateMemorySize64);
        }

        /// <summary>
        /// Values the server was started with (configuration entries set explicitly, not defaults).
        /// </summary>
        public async Task<Dictionary<string, string>> GetServerValuesAsync(IEnumerable<string> keys)
        {
            HashSet<string> wanted = new(keys, StringComparer.OrdinalIgnoreCase);
            Dictionary<string, string> values = new(StringComparer.OrdinalIgnoreCase);
            using JsonDocument document = JsonDocument.Parse(await Http.GetStringAsync($"{Url}/admin/configuration/settings"));
            foreach (JsonElement setting in document.RootElement.GetProperty("Settings").EnumerateArray())
            {
                if (setting.TryGetProperty("ServerValues", out JsonElement serverValues) == false || serverValues.ValueKind != JsonValueKind.Object)
                    continue;

                foreach (JsonProperty value in serverValues.EnumerateObject())
                {
                    if (wanted.Contains(value.Name) && value.Value.TryGetProperty("Value", out JsonElement v))
                        values[value.Name] = v.GetString();
                }
            }

            return values;
        }

        public async Task<string> GetLicenseAsync()
        {
            using JsonDocument document = JsonDocument.Parse(await Http.GetStringAsync($"{Url}/license/status"));
            JsonElement root = document.RootElement;
            return $"{root.GetProperty("Type").GetString()}, {root.GetProperty("MaxCores").GetInt32()} cores";
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                if (_process.HasExited == false)
                {
                    await _process.StandardInput.WriteLineAsync("shutdown no-confirmation");
                    await _process.StandardInput.FlushAsync();
                    using CancellationTokenSource timeout = new(TimeSpan.FromMinutes(2));
                    try
                    {
                        await _process.WaitForExitAsync(timeout.Token);
                    }
                    catch (OperationCanceledException)
                    {
                        WriteLog(_log, "==== graceful shutdown timed out, killing the server");
                        _process.Kill(entireProcessTree: true);
                        await _process.WaitForExitAsync();
                    }
                }
            }
            finally
            {
                _process.Dispose();
                lock (_log)
                    _log.Dispose();
            }
        }

        private async Task WaitUntilReadyAsync(TimeSpan timeout)
        {
            Stopwatch sw = Stopwatch.StartNew();
            while (true)
            {
                if (_process.HasExited)
                    throw new InvalidOperationException($"The server exited with code {_process.ExitCode} during startup, see the server log");

                try
                {
                    using HttpResponseMessage response = await Http.GetAsync($"{Url}/build/version");
                    if (response.IsSuccessStatusCode)
                        return;
                }
                catch (HttpRequestException)
                {
                }

                if (sw.Elapsed > timeout)
                    throw new TimeoutException($"The server did not start within {timeout}");

                await Task.Delay(250);
            }
        }

        private static void WriteLog(StreamWriter log, string line)
        {
            if (line == null)
                return;

            lock (log)
            {
                if (log.BaseStream != null)
                    log.WriteLine(line);
            }
        }
    }

    /// <summary>
    /// Samples the private bytes of a process in the background and keeps the peak.
    /// </summary>
    internal sealed class PeakMemorySampler : IAsyncDisposable
    {
        private readonly Process _process;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _loop;

        public PeakMemorySampler(int processId, TimeSpan interval)
        {
            _process = Process.GetProcessById(processId);
            _loop = Task.Run(async () =>
            {
                while (_cts.IsCancellationRequested == false)
                {
                    Sample();
                    try
                    {
                        await Task.Delay(interval, _cts.Token);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }
            });
        }

        public long PeakPrivateBytes { get; private set; }

        private void Sample()
        {
            _process.Refresh();
            PeakPrivateBytes = Math.Max(PeakPrivateBytes, _process.PrivateMemorySize64);
        }

        public async ValueTask DisposeAsync()
        {
            _cts.Cancel();
            await _loop;
            Sample();
            _process.Dispose();
            _cts.Dispose();
        }
    }
}
