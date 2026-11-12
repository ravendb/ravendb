using System;
using System.Reflection;
using System.Runtime.Versioning;
using System.ServiceProcess;
using System.Threading;
using Raven.Server.Config;
using Raven.Server.Logging;
using Raven.Server.Utils.Cli;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server.Logging;

namespace Raven.Server.Utils
{
    public static class WindowsServiceRunner
    {
        public static void Run(RavenConfiguration configuration, string[] args)
        {
#pragma warning disable CA1416 // Validate platform compatibility
            var service = new RavenWin32Service(configuration, args);
            Program.IsRunningAsWindowsService = true;
            Program.RestartServer = service.RequestRestart;
            Program.ShutdownServer = service.RequestStop;
            ServiceBase.Run(service);
#pragma warning restore CA1416 // Validate platform compatibility
        }

        public static bool ShouldRunAsWindowsService()
        {
            if (PlatformDetails.RunningOnPosix)
                return false;

            using (var p = ParentProcessUtilities.GetParentProcess())
            {
                if (p == null)
                    return false;
                var hasBeenStartedByServices = p.ProcessName == "services";
                return hasBeenStartedByServices;
            }
        }
    }

    [SupportedOSPlatform("windows")]
    internal sealed class RavenWin32Service : ServiceBase
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<RavenWin32Service>();

        // ServiceBase has no public API for preshutdown notifications, so SERVICE_ACCEPT_PRESHUTDOWN is added to the controls
        // it reports as accepted and SERVICE_CONTROL_PRESHUTDOWN (which ServiceBase passes to OnCustomCommand) is handled there
        internal static readonly FieldInfo AcceptedCommandsField = typeof(ServiceBase).GetField("_acceptedCommands", BindingFlags.Instance | BindingFlags.NonPublic);

        private const int ServiceAcceptPreshutdown = 0x00000100;

        private const int ServiceControlPreshutdown = 0x0000000F;

        private const int ErrorExceptionInService = 1064;

        private static readonly TimeSpan PendingProgressInterval = TimeSpan.FromSeconds(5);

        private static readonly TimeSpan PendingProgressWaitHint = TimeSpan.FromSeconds(30);

        private readonly object _locker = new object();

        private readonly string[] _args;

        private RavenConfiguration _configuration;

        private RavenServer _ravenServer;

        private bool _stopped;

        private bool _systemShutdown;

        public RavenWin32Service(RavenConfiguration configuration, string[] args)
        {
            // ServiceName is intentionally left empty - for an own process service ServiceBase takes it from the SCM
            // (it is the first ServiceMain argument), so it is always the name the service was registered under
            // and is not subject to ServiceBase's 80 characters limit (the SCM allows up to 256)

            // RavenDB has its own logging and LocalService is not allowed to create an event log source
            AutoLog = false;

            AcceptPreshutdown();

            _args = args;
            _configuration = configuration;
            _ravenServer = new RavenServer(configuration);
        }

        private void AcceptPreshutdown()
        {
            if (AcceptedCommandsField?.GetValue(this) is int acceptedCommands)
            {
                AcceptedCommandsField.SetValue(this, acceptedCommands | ServiceAcceptPreshutdown);
                return;
            }

            if (Logger.IsInfoEnabled)
                Logger.Info("Unable to register for preshutdown notifications, the service is going to be stopped on SERVICE_CONTROL_SHUTDOWN instead.");
        }

        protected override void OnStart(string[] args)
        {
            using (new PendingProgressReporter(this))
            {
                StartServer();
            }
        }

        private void StartServer()
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Starting RavenDB Windows Service: {ServiceName}.");

            try
            {
                _ravenServer.OpenPipes();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Unable to OpenPipe. Admin Channel will not be available to the user", e);

                throw;
            }

            try
            {
                _ravenServer.Initialize();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error initializing the server", e);

                throw;
            }

            Program.OnServerInitialized(_ravenServer);
        }

        public void RequestRestart()
        {
            // the restart disposes the current server (along with the admin channel or the request that asked for it),
            // so it cannot run on the caller's thread
            ThreadPool.QueueUserWorkItem(_ => Restart());
        }

        public void RequestStop()
        {
            ThreadPool.QueueUserWorkItem(_ => Stop());
        }

        private void Restart()
        {
            var failed = false;

            lock (_locker)
            {
                if (_stopped)
                    return;

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Restarting RavenDB Windows Service: {ServiceName}.");

                try
                {
                    _ravenServer.Dispose();

                    var configurationBeforeRestart = _configuration;
                    var configuration = RavenConfiguration.CreateForServer(null, CommandLineSwitches.CustomConfigPath);

                    if (_args != null)
                        configuration.AddCommandLine(PostSetupCliArgumentsUpdater.Process(_args, configurationBeforeRestart, configuration));

                    configuration.Initialize();

                    Program.OnServerRestarted(configurationBeforeRestart.Core.DataDirectory.FullPath, configuration.Core.DataDirectory.FullPath);

                    _configuration = configuration;
                    _ravenServer = new RavenServer(configuration);

                    StartServer();
                }
                catch (Exception e)
                {
                    if (Logger.IsErrorEnabled)
                        Logger.Error($"Failed to restart RavenDB Windows Service: {ServiceName}. Stopping the service.", e);

                    failed = true;
                }
            }

            if (failed)
            {
                // report the failure to the SCM, so it can apply the configured recovery actions
                ExitCode = ErrorExceptionInService;
                Stop();
            }
        }

        protected override void OnStop()
        {
            using (new PendingProgressReporter(this))
            {
                if (Logger.IsInfoEnabled)
                {
                    Logger.Info(_systemShutdown
                        ? $"Stopping RavenDB Windows Service: {ServiceName} due to system shutdown (preshutdown notification)."
                        : $"Stopping RavenDB Windows Service: {ServiceName}.");

                    if (_systemShutdown == false)
                        Thread.Sleep(3000);
                }

                StopServer();
            }
        }

        protected override void OnCustomCommand(int command)
        {
            if (command != ServiceControlPreshutdown)
            {
                base.OnCustomCommand(command);
                return;
            }

            // the SCM waits until the service is stopped (or the configured preshutdown timeout passes) before it continues the shutdown,
            // so the server can be disposed gracefully and the progress is reported to the SCM in the meantime
            _systemShutdown = true;
            Stop();
        }

        protected override void OnShutdown()
        {
            // used only when the preshutdown notifications are not available - ServiceBase accepts SERVICE_CONTROL_SHUTDOWN for every
            // stoppable service and reports it as stopped once this returns (which lets the process exit), so the server needs to be
            // disposed here as well, but there are only a few seconds (WaitToKillServiceTimeout) for that and no way to ask for more
            if (Logger.IsInfoEnabled)
                Logger.Info($"Stopping RavenDB Windows Service: {ServiceName} due to system shutdown (shutdown notification).");

            StopServer();
        }

        private void StopServer()
        {
            lock (_locker)
            {
                _stopped = true;
                _ravenServer.Dispose();
            }

            if (Logger.IsInfoEnabled)
                Logger.Info($"RavenDB Windows Service: {ServiceName} stopped.");
        }

        // keeps telling the SCM that a pending start or stop is progressing, so it does not consider the service unresponsive
        // (e.g. 'The service did not respond to the start or control request in a timely fashion') when it takes a while
        private sealed class PendingProgressReporter : IDisposable
        {
            private readonly RavenWin32Service _service;

            private readonly Timer _timer;

            private readonly object _locker = new object();

            private bool _disposed;

            public PendingProgressReporter(RavenWin32Service service)
            {
                _service = service;
                _timer = new Timer(_ => Report(), null, TimeSpan.Zero, PendingProgressInterval);
            }

            private void Report()
            {
                lock (_locker)
                {
                    if (_disposed)
                        return;

                    try
                    {
                        _service.RequestAdditionalTime(PendingProgressWaitHint);
                    }
                    catch (InvalidOperationException)
                    {
                        // the service is not in a pending state, there is nothing to report
                    }
                }
            }

            public void Dispose()
            {
                lock (_locker)
                {
                    _disposed = true;
                }

                _timer.Dispose();
            }
        }
    }
}
