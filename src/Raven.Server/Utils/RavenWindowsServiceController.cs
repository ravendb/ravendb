using System;
using System.Collections.Generic;
using System.ComponentModel;
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
        private const uint ErrorServiceExists = 0x00000431;

        private const uint ErrorServiceMarkedForDeletion = 0x00000430;

        private const uint ErrorAccessIsDenied = 0x00000005;

        private static string ServiceFullName => $@"""{RavenWindowsService.WindowsServiceName} ({RavenWindowsService.WindowServiceDescription})""";

        public static void Install(string[] args)
        {
            var serviceArgs = args.Where(x => x != "--register-service").ToList();
            using (var serviceController = GetServiceController())
            {
                InstallInternal(serviceController, serviceArgs);
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

                Console.WriteLine($"Service {ServiceFullName} has been registered.");
            }
            catch (Win32Exception e) when (e.NativeErrorCode == ErrorServiceExists)
            {
                Console.WriteLine($"Service {ServiceFullName} already exists. Reinstalling...");
                Reinstall(serviceController, serviceArgs);
            }
            catch (Win32Exception e) when (e.NativeErrorCode == ErrorServiceMarkedForDeletion)
            {
                if (counter < 10)
                {
                    Console.WriteLine($"Service {ServiceFullName} has been marked for deletion.  Performing {counter + 1} installation attempt.");

                    Thread.Sleep(1000);
                    counter++;

                    InstallInternal(serviceController, serviceArgs, counter);
                }
            }
            catch (Win32Exception e) when (e.NativeErrorCode == ErrorAccessIsDenied)
            {
                Console.WriteLine($"Cannot register service {ServiceFullName} due to insufficient privileges. Please use Administrator account to install the service.");
            }
            catch (Win32Exception e)
            {
                Console.WriteLine($"Cannot register service {ServiceFullName}: { FormatWin32ErrorMessage(e) }");
            }
        }

        private static string FormatWin32ErrorMessage(Win32Exception exception)
        {
            return $"{exception.Message} (ERROR CODE 0x{exception.NativeErrorCode:x8}).";
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
            if (serviceController == null)
            {
                Console.WriteLine($"Service {ServiceFullName} does not exist. No action taken.");
                return;
            }

            if ((serviceController.Status == ServiceControllerStatus.Stopped || serviceController.Status == ServiceControllerStatus.StopPending) == false)
            {
                try
                {
                    StopInternal(serviceController);
                }
                catch (InvalidOperationException invalidOperationException)
                {
                    var win32Exception = invalidOperationException.InnerException as Win32Exception;
                    if (win32Exception == null)
                        throw;

                    Console.WriteLine($"Error stopping service {ServiceFullName}: { FormatWin32ErrorMessage(win32Exception) }");
                    return;
                }
            }

            try
            {
                new Win32ServiceManager().DeleteService(
                    NormalizeServiceName(RavenWindowsService.WindowsServiceName));

                Console.WriteLine($"Service {ServiceFullName} has been unregistered.");
            }
            catch (Win32Exception exception) when (exception.NativeErrorCode == ErrorAccessIsDenied)
            {
                Console.WriteLine($"Cannot unregister service {ServiceFullName} due to insufficient privileges. Please use Administrator account to uninstall the service.");
            }
            catch (Win32Exception exception)
            {
                Console.WriteLine($"Cannot unregister service {ServiceFullName}: { FormatWin32ErrorMessage(exception) }");
            }
        }

        private static void Reinstall(ServiceController serviceController, List<string> serviceArgs)
        {
            StopInternal(serviceController);
            UninstallInternal(serviceController);
            InstallInternal(serviceController, serviceArgs);
        }

        private static void StopInternal(ServiceController serviceController)
        {
            if (serviceController == null)
                return;

            if (!(serviceController.Status == ServiceControllerStatus.Stopped | serviceController.Status == ServiceControllerStatus.StopPending))
            {
                Console.WriteLine($"Service {ServiceFullName} is being stopped.");
                serviceController.Stop();
            }

            serviceController.WaitForStatus(
                ServiceControllerStatus.Stopped, TimeSpan.FromSeconds(60));

            Console.WriteLine($"Service {ServiceFullName} stopped.");
        }

        public static void Run(RavenConfiguration configuration)
        {
            var service = new RavenWindowsService(configuration);
            var serviceHost = new Win32ServiceHost(service);
            serviceHost.Run();
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
            var serviceName = NormalizeServiceName(RavenWindowsService.WindowsServiceName);
            return ServiceController.GetServices()
                .FirstOrDefault(x => x.ServiceName == serviceName);
        }

        public static bool ShouldRunAsWindowsService(RavenConfiguration configuration)
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
            _ravenServer.AfterDisposal += () => serviceStoppedCallback();
        }

        public void Stop()
        {
            _ravenServer.Dispose();
        }
    }
}
