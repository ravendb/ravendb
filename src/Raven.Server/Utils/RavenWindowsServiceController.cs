using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Loader;
using System.ServiceProcess;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using DasMulli.Win32.ServiceUtils;
using Microsoft.Extensions.PlatformAbstractions;
using Raven.Server.Config;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Utils
{
    public static class RavenWindowsServiceController
    {
        private static string ServiceFullName => $@"""{RavenWindowsService.WindowsServiceName}"" (""{RavenWindowsService.WindowServiceDescription}"")";

        public static void Install(string[] args)
        {
            var argsForService = args.Where(x => x != "--register-service").ToList();
            using (var serviceController = GetServiceController())
            {
                InstallInternal(serviceController, argsForService);
            }
        }

        private static void InstallInternal(
            ServiceController serviceController, List<string> serviceArgs, int counter = 0)
        {
            var serviceCommand = GetServiceCommand(serviceArgs);
            var serviceName = RavenWindowsService.WindowsServiceName;
            var serviceDesc = RavenWindowsService.WindowServiceDescription;

            try
            {
                new Win32ServiceManager().CreateService(
                    NormalizeServiceName(serviceName),
                    serviceName,
                    serviceDesc,
                    serviceCommand,
                    Win32ServiceCredentials.LocalSystem,
                    autoStart: true,
                    startImmediately: true,
                    errorSeverity: ErrorSeverity.Normal);

                Console.WriteLine($@"Successfully registered service {ServiceFullName}");
            }
            catch (Exception e) 
                when (e.Message.Contains("already exists"))
            {
                Console.WriteLine($@"Service {ServiceFullName} was already installed. Reinstalling...");

                Reinstall(serviceController, serviceArgs);

            } catch (Exception e) 
                when (e.Message.Contains("The specified service has been marked for deletion"))
            {
                if (counter < 10)
                {
                    Thread.Sleep(500);
                    counter++;

                    Console.WriteLine("The specified service has been marked for deletion. Retrying {0} time", counter);
                    InstallInternal(serviceController, serviceArgs, counter);
                }
            }

        }

        private static string NormalizeServiceName(string serviceName)
        {
            return Regex.Replace(serviceName, @"[\/\s]", "_");
        }

        public static void Uninstall()
        {
            using (var serviceController = GetServiceController())
            {
                UninstallInternal(serviceController);
            }
        }

        private static void UninstallInternal(ServiceController serviceController)
        {
            try
            {
                if ((serviceController.Status == ServiceControllerStatus.Stopped || serviceController.Status == ServiceControllerStatus.StopPending) == false)
                {
                    StopInternal(serviceController);
                }

                new Win32ServiceManager().DeleteService(
                    NormalizeServiceName(RavenWindowsService.WindowsServiceName));

                Console.WriteLine($@"Successfully unregistered service {ServiceFullName}");
            }
            catch (Exception e) when (e.Message.Contains("does not exist"))
            {
                Console.WriteLine($@"Service {ServiceFullName} does not exist. No action taken.");
            }
        }

        private static void Reinstall(ServiceController sc, List<string> serviceArgs)
        {
            StopInternal(sc);
            UninstallInternal(sc);
            InstallInternal(sc, serviceArgs);
        }

        private static void StopInternal(ServiceController serviceController)
        {
            if (!(serviceController.Status == ServiceControllerStatus.Stopped | serviceController.Status == ServiceControllerStatus.StopPending))
            {
                serviceController.Stop();
                serviceController.WaitForStatus(ServiceControllerStatus.Stopped, TimeSpan.FromMilliseconds(1000));
                Console.WriteLine($@"Successfully stopped service {ServiceFullName}");
            }
            else
            {
                Console.WriteLine($@"Service {ServiceFullName} is already stopped or stop is pending.");
            }
        }

        public static void Stop()
        {
            using (var serviceController = GetServiceController())
            {
                StopInternal(serviceController);
            }
        }

        public static void Start()
        {
            using (var serviceController = GetServiceController())
            {
                StartInternal(serviceController);
            }
        }

        public static void Run(RavenConfiguration configuration)
        {
            var service = new RavenWindowsService(configuration);
            var serviceHost = new Win32ServiceHost(service);
            serviceHost.Run();
        }

        private static void StartInternal(ServiceController serviceController)
        {
            if (!(serviceController.Status == ServiceControllerStatus.StartPending | serviceController.Status == ServiceControllerStatus.Running))
            {
                serviceController.Start();
                serviceController.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromMilliseconds(1000));
                Console.WriteLine($@"Successfully started service {ServiceFullName}");
            }
            else
            {
                Console.WriteLine($@"Service {ServiceFullName} is already running or start is pending.");
            }
        }

        private static string GetServiceCommand(List<string> argsForService)
        {
            var result = Process.GetCurrentProcess().MainModule.FileName;
            if (result.EndsWith("dotnet.exe", StringComparison.OrdinalIgnoreCase))
            {
                var appPath = Path.Combine(PlatformServices.Default.Application.ApplicationBasePath,
                    PlatformServices.Default.Application.ApplicationName + ".dll");
                result = string.Format("{0} \"{1}\"", result, appPath);
            }

            if (argsForService.Any())
            {
                result = $"{result} { string.Join(" ", argsForService) }";
            }

            return result;
        }

        private static ServiceController GetServiceController()
        {
            return new ServiceController(NormalizeServiceName(RavenWindowsService.WindowsServiceName));
        }

        public static bool ShouldRunAsWindowsService(RavenConfiguration configuration)
        {
            if (PlatformDetails.RunningOnPosix)
                return false;

            var p = ParentProcessUtilities.GetParentProcess();
            var hasBeenStartedByServices = p.ProcessName == "services";
            return hasBeenStartedByServices;
        }
    }

    internal class RavenWindowsService : IWin32Service
    {
        public const string WindowServiceDescription = "Next generation NoSQL Database";

        public static string WindowsServiceName => CommandLineSwitches.ServiceName;

        private readonly RavenServer _ravenServer;

        public string ServiceName => WindowsServiceName;

        public RavenWindowsService(RavenConfiguration configuration)
        {
            _ravenServer = new RavenServer(configuration);
        }

        public void Start(string[] startupArguments, ServiceStoppedCallback serviceStoppedCallback)
        {
            _ravenServer.Initialize();
        }

        public void Stop()
        {
            _ravenServer.Dispose();
        }
    }
}
