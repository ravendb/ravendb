using System;
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
            Program.RestartServer = service.Restart;
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

        private RavenServer _ravenServer;

        private readonly string[] _args;

        public RavenWin32Service(RavenConfiguration configuration, string[] args)
        {
            // ServiceName is intentionally left empty - for an own process service ServiceBase takes it from the SCM
            // (it is the first ServiceMain argument), so it is always the name the service was registered under
            // and is not subject to ServiceBase's 80 characters limit (the SCM allows up to 256)

            // RavenDB has its own logging and LocalService is not allowed to create an event log source
            AutoLog = false;

            _args = args;
            _ravenServer = new RavenServer(configuration);
        }

        protected override void OnStart(string[] args)
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
        }

        public void Restart()
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Restarting RavenDB Windows Service: {ServiceName}.");

            _ravenServer.Dispose();
            var configuration = RavenConfiguration.CreateForServer(null, CommandLineSwitches.CustomConfigPath);

            if (_args != null)
                configuration.AddCommandLine(_args);

            configuration.Initialize();
            _ravenServer = new RavenServer(configuration);
            OnStart(_args);
        }

        protected override void OnStop()
        {
            if (Logger.IsInfoEnabled)
            {
                Logger.Info($"Stopping RavenDB Windows Service: {ServiceName}.");

                Thread.Sleep(3000);
            }

            _ravenServer.Dispose();
        }

        protected override void OnShutdown()
        {
            // ServiceBase accepts SERVICE_CONTROL_SHUTDOWN for every stoppable service and reports it as stopped
            // once this returns (which lets the process exit), so the server needs to be disposed here as well
            if (Logger.IsInfoEnabled)
                Logger.Info($"Stopping RavenDB Windows Service: {ServiceName} due to system shutdown.");

            _ravenServer.Dispose();
        }
    }
}
