#nullable enable

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

#if !NET462

using System.Runtime.Loader;

#endif

using System.Text;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Http;
using Sparrow.Logging;
using Raven.Client.Util;
using Sparrow.Platform;

namespace Raven.Embedded
{
    public sealed class EmbeddedServer : IDisposable
    {
        public static EmbeddedServer Instance = new EmbeddedServer();

        internal EmbeddedServer()
        {
        }

        private readonly Logger _logger = LoggingSource.Instance.GetLogger<EmbeddedServer>("Embedded");
        private Lazy<Task<(Uri ServerUrl, Process ServerProcess)>>? _serverTask;

        private readonly ConcurrentDictionary<string, Lazy<Task<IDocumentStore>>> _documentStores = new ConcurrentDictionary<string, Lazy<Task<IDocumentStore>>>();

        private ServerOptions? _serverOptions;

        public async Task<int> GetServerProcessIdAsync(CancellationToken token = default)
        {
            var server = _serverTask;
            if (server == null)
                throw new InvalidOperationException($"Please run {nameof(StartServer)}() before trying to use the server");
            return (await server.Value.WithCancellation(token).ConfigureAwait(false)).ServerProcess.Id;
        }

        public async Task RestartServerAsync()
        {
            var existingServerTask = _serverTask;
            if (_serverOptions == null || existingServerTask == null || existingServerTask.IsValueCreated == false)
                throw new InvalidOperationException("Cannot call RestartServer() before calling Start()");

            try
            {
                var (serverUrl, serverProcess) = await existingServerTask.Value.ConfigureAwait(false);
                ShutdownServerProcess(serverProcess);
            }
            catch
            {
                // we will ignore errors here, the process might already be dead, we failed to start, etc
            }

            if (Interlocked.CompareExchange(ref _serverTask, null, existingServerTask) != existingServerTask)
                throw new InvalidOperationException("The server changed while restarting it. Are you calling RestartServer() concurrently?");

            await StartServerInternalAsync().ConfigureAwait(false);
        }

        public void StartServer(ServerOptions? options = null)
        {
            _serverOptions = options ??= ServerOptions.Default;

            if (options.Security != null)
            {
                try
                {
                    var thumbprint = options.Security.ServerCertificateThumbprint;
                    RequestExecutor.RemoteCertificateValidationCallback += (sender, certificate, chain, errors) =>
                    {
#if NET6_0_OR_GREATER
                        if (certificate == null)
                            return false;
#endif
                        var certificate2 = certificate as X509Certificate2 ?? new X509Certificate2(certificate);
                        return certificate2.Thumbprint == thumbprint;
                    };
                }
                catch (NotSupportedException)
                {
                    // not supported on Mono
                }
                catch (InvalidOperationException)
                {
                    // not supported on MacOSX
                }
            }

            var task = StartServerInternalAsync();
            GC.KeepAlive(task); // make sure that we aren't allowing to elide the call
        }

        private Task StartServerInternalAsync()
        {
            var startServer = new Lazy<Task<(Uri ServerUrl, Process ServerProcess)>>(RunServer);
            if (Interlocked.CompareExchange(ref _serverTask, startServer, null) != null)
                throw new InvalidOperationException("The server was already started");

            // this forces the server to start running in an async manner.
            return startServer.Value;
        }

        public IDocumentStore GetDocumentStore(string database)
        {
            return AsyncHelpers.RunSync(() => GetDocumentStoreAsync(database));
        }

        public IDocumentStore GetDocumentStore(DatabaseOptions options)
        {
            return AsyncHelpers.RunSync(() => GetDocumentStoreAsync(options));
        }

        public Task<IDocumentStore> GetDocumentStoreAsync(string database, CancellationToken token = default)
        {
            return GetDocumentStoreAsync(new DatabaseOptions(database), token);
        }

        public async Task<IDocumentStore> GetDocumentStoreAsync(DatabaseOptions options, CancellationToken token = default)
        {
            var databaseName = options.DatabaseRecord.DatabaseName;
            if (string.IsNullOrWhiteSpace(databaseName))
                throw new ArgumentNullException(nameof(options.DatabaseRecord.DatabaseName), "The database name is mandatory");

            if (_serverOptions == null)
                throw new InvalidOperationException("Cannot get document document before the server was started");

            if (_logger.IsInfoEnabled)
                _logger.Info($"Creating document store for '{databaseName}'.");

            token.ThrowIfCancellationRequested();

            var lazy = new Lazy<Task<IDocumentStore>>(async () =>
            {
                var serverUrl = await GetServerUriAsync(token).ConfigureAwait(false);
                var store = new DocumentStore
                {
                    Urls = new[] { serverUrl.AbsoluteUri },
                    Database = databaseName,
                    Certificate = _serverOptions!.Security?.ClientCertificate,
                    Conventions = options.Conventions
                };

                store.Conventions.DisableTopologyCache = true;

                store.AfterDispose += (sender, args) => _documentStores.TryRemove(databaseName, out _);

                store.Initialize();
                if (options.SkipCreatingDatabase == false)
                    await TryCreateDatabase(options, store, token).ConfigureAwait(false);

                return store;
            });

            return await _documentStores.GetOrAdd(databaseName, lazy).Value.WithCancellation(token).ConfigureAwait(false);
        }

        private async Task TryCreateDatabase(DatabaseOptions options, IDocumentStore store, CancellationToken token)
        {
            try
            {
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(options.DatabaseRecord), token).ConfigureAwait(false);
            }
            catch (ConcurrencyException)
            {
                // Expected behaviour when the database is already exists

                if (_logger.IsInfoEnabled)
                    _logger.Info($"{options.DatabaseRecord.DatabaseName} already exists.");
            }
        }

        public async Task<Uri> GetServerUriAsync(CancellationToken token = default)
        {
            var server = _serverTask;
            if (server == null)
                throw new InvalidOperationException($"Please run {nameof(StartServer)}() before trying to use the server");

            return (await server.Value.WithCancellation(token).ConfigureAwait(false)).ServerUrl;
        }

        private void ShutdownServerProcess(Process process)
        {
            if (process == null || process.HasExited)
                return;

            lock (process)
            {
                if (process.HasExited)
                    return;

                try
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Try shutdown server PID {process.Id} gracefully.");

                    using (var inputStream = process.StandardInput)
                    {
                        inputStream.WriteLine("shutdown no-confirmation");
                    }

                    if (process.WaitForExit((int)_serverOptions!.GracefulShutdownTimeout.TotalMilliseconds))
                        return;
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Failed to shutdown server PID {process.Id} gracefully in {_serverOptions!.GracefulShutdownTimeout.ToString()}", e);
                    }
                }

                try
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Killing global server PID {process.Id}.");

                    process.Kill();
                    if (process.WaitForExit((int)_serverOptions!.ProcessKillTimeout.TotalMilliseconds) == false)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Process {process.Id} did not exit after Kill() within {_serverOptions!.ProcessKillTimeout.ToString()} seconds.");
                    }
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info($"Failed to kill process {process.Id}", e);
                    }
                }
            }
        }

        public event EventHandler<ServerProcessExitedEventArgs>? ServerProcessExited;

        private async Task<(Uri ServerUrl, Process ServerProcess)> RunServer()
        {
            if (_serverOptions == null)
                throw new ArgumentNullException(nameof(_serverOptions));

            var process = await RavenServerRunner.RunAsync(_serverOptions).ConfigureAwait(false);
            if (_logger.IsInfoEnabled)
                _logger.Info($"Starting global server: {process.Id}");

            process.Exited += (sender, e) => ServerProcessExited?.Invoke(sender, new ServerProcessExitedEventArgs());

#if NET462
            AppDomain.CurrentDomain.DomainUnload += (s, args) =>
            {
                ShutdownServerProcess(process);
            };
#else
            AssemblyLoadContext.Default.Unloading += c =>
            {
                ShutdownServerProcess(process);
            };
#endif

            var serverUrlTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            var outputString = process.ReadOutput(onOutputLine: line =>
            {
                const string prefix = "Server available on: ";
                if (line.StartsWith(prefix))
                    serverUrlTcs.TrySetResult(line.Substring(prefix.Length));
            });

            try
            {
                var startupTask = Task.WhenAny(serverUrlTcs.Task, outputString.Completion);
                var isCompleted = await startupTask.WaitWithTimeout(_serverOptions.MaxServerStartupTimeDuration).ConfigureAwait(false);
                var exitCode = outputString.ExitCode;

                if (isCompleted == false || serverUrlTcs.Task.IsCompleted == false || exitCode.HasValue)
                {
                    throw new InvalidOperationException(isCompleted == false
                        ? $"Server startup did not complete within {_serverOptions.MaxServerStartupTimeDuration}."
                        : "The server process exited before startup completed.");
                }

                var serverUrl = new Uri(await serverUrlTcs.Task.ConfigureAwait(false));

                return (serverUrl, process);
            }
            catch (Exception e)
            {
                try
                {
                    // Capture the exit code before cleanup can terminate a still-running process.
                    var exitCodeBeforeShutdown = outputString.ExitCode;

                    ShutdownServerProcess(process);
                    await outputString.DrainAsync(_serverOptions.ProcessKillTimeout).ConfigureAwait(false);

                    var message = new StringBuilder();
                    message.AppendLine("Unable to start the RavenDB Server");
                    message.AppendLine(e.Message);
                    outputString.AppendDiagnostics(message, exitCodeBeforeShutdown);

                    throw new InvalidOperationException(message.ToString(), e);
                }
                finally
                {
                    process.Dispose();
                }
            }
        }

        public void OpenStudioInBrowser()
        {
            var serverUrl = AsyncHelpers.RunSync(() => GetServerUriAsync());
            var url = serverUrl.AbsoluteUri + "studio/index.html?disableAnalytics=true";

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

        public void Dispose()
        {
            var lazy = Interlocked.Exchange(ref _serverTask, null);
            if (lazy == null || lazy.IsValueCreated == false)
                return;

            var process = lazy.Value.Result.ServerProcess;
            ShutdownServerProcess(process);
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
