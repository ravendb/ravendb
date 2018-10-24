using System;
using DasMulli.Win32.ServiceUtils;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Raven.Server.Utils.Cli;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Utils
{
    public static class WindowsServiceRunner
    {
        public static void Run(string serviceName, RavenConfiguration configuration, string[] args)
        {
            var service = new RavenWin32Service(serviceName, configuration, args);
            Program.RestartServer = service.Restart;
            var serviceHost = new Win32ServiceHost(service);
            serviceHost.Run();
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

    internal class RavenWin32Service : IWin32Service
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<RavenWin32Service>("Server");

        private RavenServer _ravenServer;

        private readonly string[] _args;

        public string ServiceName { get; }

        private ServiceStoppedCallback _serviceStoppedCallback;

        public RavenWin32Service(string serviceName, RavenConfiguration configuration, string[] args)
        {
            ServiceName = serviceName;
            _args = args;
            _ravenServer = new RavenServer(configuration);
        }

        public void Start(string[] startupArguments, ServiceStoppedCallback serviceStoppedCallback)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Starting RavenDB Windows Service: {ServiceName}.");

            _serviceStoppedCallback = serviceStoppedCallback;

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
            Start(_args, _serviceStoppedCallback);

            configuration.Initialize();
        }

        public void Stop()
        {
            if (Logger.IsOperationsEnabled)
                Logger.OperationsAsync($"Stopping RavenDB Windows Service: {ServiceName}.").Wait(TimeSpan.FromSeconds(15));

            _ravenServer.Dispose();
            _serviceStoppedCallback();
        }
    }
}
