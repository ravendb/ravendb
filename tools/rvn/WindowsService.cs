using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text.RegularExpressions;
using System.Threading;
using DasMulli.Win32.ServiceUtils;

namespace rvn
{
    public static class WindowsService
    {
        public static void Register(string serviceName, string username, string password, string ravenServerDir, List<string> args)
        {
            new WindowsServiceController(serviceName, username, password).Install(ravenServerDir, args);
        }

        public static void Unregister(string serviceName)
        {
            new WindowsServiceController(serviceName, username: null, password: null).Uninstall();
        }

        public static void Start(string serviceName)
        {
            new WindowsServiceController(serviceName, username: null, password: null).Start();
        }

        public static void Stop(string serviceName)
        {
            new WindowsServiceController(serviceName, username: null, password: null).Stop();
        }

        internal class WindowsServiceController
        {
            public const string WindowServiceDescription = "Next generation NoSQL Database";

            private const uint ErrorServiceExists = 0x00000431;

            private const uint ErrorServiceMarkedForDeletion = 0x00000430;

            private const uint ErrorAccessIsDenied = 0x00000005;

            private readonly string _serviceName;
            private readonly string _username;
            private readonly string _password;

            private string ServiceFullName => $@"""{_serviceName} ({WindowServiceDescription})""";

            public WindowsServiceController(string serviceName, string username, string password)
            {
                _serviceName = serviceName;
                _username = username;
                _password = password;
            }

            public void Install(string ravenServerDir, List<string> args)
            {
                using (var serviceController = GetServiceController())
                {
                    InstallInternal(serviceController, ravenServerDir, args);
                }
            }

            private void InstallInternal(
                ServiceController serviceController, string ravenServerDir, List<string> serviceArgs, int counter = 0)
            {
                var serviceName = _serviceName;
                var serviceCommand = GetServiceCommand(ravenServerDir, serviceArgs);
                var serviceDesc = WindowServiceDescription;

                try
                {
                    var credentials = Win32ServiceCredentials.LocalService;
                    if (string.IsNullOrWhiteSpace(_username) == false)
                        credentials = new Win32ServiceCredentials(_username, _password);

                    new Win32ServiceManager().CreateService(new ServiceDefinition(NormalizeServiceName(serviceName), serviceCommand)
                    {
                        DisplayName = serviceName,
                        Description = serviceDesc,
                        Credentials = credentials,
                        AutoStart = true,
                        DelayedAutoStart = false,
                        ErrorSeverity = ErrorSeverity.Normal
                    });

                    Console.WriteLine($"Service {ServiceFullName} has been registered.");
                }
                catch (Win32Exception e) when (e.NativeErrorCode == ErrorServiceExists)
                {
                    Console.WriteLine($"Service {ServiceFullName} already exists. Reinstalling...");
                    Reinstall(serviceController, ravenServerDir, serviceArgs);
                }
                catch (Win32Exception e) when (e.NativeErrorCode == ErrorServiceMarkedForDeletion)
                {
                    if (counter < 10)
                    {
                        Console.WriteLine($"Service {ServiceFullName} has been marked for deletion. Performing {counter + 1} installation attempt.");

                        Thread.Sleep(1000);
                        counter++;

                        InstallInternal(serviceController, ravenServerDir, serviceArgs, counter);
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

            public void Uninstall()
            {
                using (var serviceController = GetServiceController())
                {
                    UninstallInternal(serviceController);
                }
            }

            private void UninstallInternal(ServiceController serviceController)
            {
                if (serviceController == null)
                {
                    Console.WriteLine($"Service {ServiceFullName} does not exist. No action taken.");
                    return;
                }

#pragma warning disable CA1416 // Validate platform compatibility
                if ((serviceController.Status == ServiceControllerStatus.Stopped || serviceController.Status == ServiceControllerStatus.StopPending) == false)
#pragma warning restore CA1416 // Validate platform compatibility
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
                    new Win32ServiceManager().DeleteService(NormalizeServiceName(_serviceName));
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

            private void Reinstall(ServiceController serviceController, string ravenServerDir, List<string> serviceArgs)
            {
                StopInternal(serviceController);
                UninstallInternal(serviceController);
                InstallInternal(serviceController, ravenServerDir, serviceArgs);
            }

            private void StopInternal(ServiceController serviceController)
            {
                if (serviceController == null)
                    return;

#pragma warning disable CA1416 // Validate platform compatibility
                if (!(serviceController.Status == ServiceControllerStatus.Stopped || serviceController.Status == ServiceControllerStatus.StopPending))
                {
                    Console.WriteLine($"Service {ServiceFullName} is being stopped.");
                    serviceController.Stop();
                }

                serviceController.WaitForStatus(ServiceControllerStatus.Stopped, TimeSpan.FromSeconds(60));
#pragma warning restore CA1416 // Validate platform compatibility

                Console.WriteLine($"Service {ServiceFullName} stopped.");
            }

            private void StartInternal(ServiceController serviceController)
            {
                if (serviceController == null)
                    return;

#pragma warning disable CA1416 // Validate platform compatibility
                if (!(serviceController.Status == ServiceControllerStatus.Running | serviceController.Status == ServiceControllerStatus.StartPending))
                {
                    Console.WriteLine($"Service {ServiceFullName} is starting.");
                    serviceController.Start();
                }

                serviceController.WaitForStatus(ServiceControllerStatus.Running, TimeSpan.FromSeconds(60));
#pragma warning restore CA1416 // Validate platform compatibility

                Console.WriteLine($"Service {ServiceFullName} started.");
            }

            public void Start()
            {
                using (var serviceController = GetServiceController())
                {
                    StartInternal(serviceController);
                }
            }

            public void Stop()
            {
                using (var serviceController = GetServiceController())
                {
                    StopInternal(serviceController);
                }
            }

            private string GetServiceCommand(string serverDir, List<string> argsForService)
            {
                using (var process = Process.GetCurrentProcess())
                {
                    var processExeFileName = process.MainModule.FileName;

                    serverDir = string.IsNullOrEmpty(serverDir)
                        ? new FileInfo(processExeFileName).Directory.FullName
                        : serverDir;

                    var serverDirInfo = new DirectoryInfo(serverDir);
                    if (serverDirInfo.Exists == false)
                        throw new ArgumentException($"Directory does not exist: {serverDir}.");
                    else
                        serverDir = serverDirInfo.FullName;

                    var serviceCommandResult = Path.Combine(serverDirInfo.FullName, "Raven.Server.exe");
                    if (File.Exists(serviceCommandResult) == false)
                    {
                        throw new ArgumentException($"Could not find RavenDB Server executable under {serverDirInfo.FullName}.");
                    }

                    if (argsForService.Any(x => x.StartsWith("--service-name")) == false)
                    {
                        argsForService.Add("--service-name");
                        argsForService.Add($"\"{_serviceName}\"");
                    }

                    serviceCommandResult = $"{serviceCommandResult} {string.Join(" ", argsForService)}";

                    return serviceCommandResult;
                }
            }

            private ServiceController GetServiceController()
            {
                var serviceName = NormalizeServiceName(_serviceName);
#pragma warning disable CA1416 // Validate platform compatibility
                return ServiceController.GetServices().FirstOrDefault(x => x.ServiceName == serviceName);
#pragma warning restore CA1416 // Validate platform compatibility
            }
        }
    }
}
