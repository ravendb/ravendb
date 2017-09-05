using System;
using DasMulli.Win32.ServiceUtils;
using Raven.Server.Config;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Utils
{
    public static class WindowsServiceRunner
    {
        public static void Run(string serviceName, RavenConfiguration configuration)
        {
            var service = new RavenWin32Service(serviceName, configuration);
            var serviceHost = new Win32ServiceHost(service);
            serviceHost.Run();
        }

        public static bool ShouldRunAsWindowsService()
        {
            if (PlatformDetails.RunningOnPosix)
                return false;

            var p = ParentProcessUtilities.GetParentProcess();
            if (p == null)
                return false;
            var hasBeenStartedByServices = p.ProcessName == "services";
            return hasBeenStartedByServices;
        }
    }

    internal class RavenWin32Service : IWin32Service
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<Program>("Raven/WindowsService");

        private readonly RavenServer _ravenServer;

        private readonly string _serviceName;

        public string ServiceName => _serviceName;

        public RavenWin32Service(string serviceName, RavenConfiguration configuration)
        {
            _serviceName = serviceName;
            _ravenServer = new RavenServer(configuration);
        }

        public void Start(string[] startupArguments, ServiceStoppedCallback serviceStoppedCallback)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Starting RavenDB Windows Service: {ServiceName}.");

            _ravenServer.AfterDisposal += () => serviceStoppedCallback();

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

        public void Stop()
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Stopping RavenDB Windows Service: {ServiceName}.");

            _ravenServer.Dispose();
        }
    }
}
