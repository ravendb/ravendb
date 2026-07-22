using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Threading;
using Sparrow.Utils;
using static Raven.Server.Utils.WindowsServiceUtils;

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

            private const string LocalServiceAccountName = @"NT AUTHORITY\LocalService";

            private static readonly string ServiceControlToolPath = Path.Combine(Environment.SystemDirectory, "sc.exe");

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
                var normalizedServiceName = NormalizeServiceName(_serviceName);
                var serviceCommand = GetServiceCommand(ravenServerDir, serviceArgs);

                var createArgs = new List<string> { "create", normalizedServiceName };

                // sc.exe expects each option as two tokens: a name ending in '=' followed by the
                // value (e.g. "type=" then "own"), never "type=own".
                void AddOption(string name, string value)
                {
                    createArgs.Add(name + "=");
                    createArgs.Add(value);
                }

                AddOption("type", "own");
                AddOption("start", "auto");
                AddOption("error", "normal");
                AddOption("binPath", serviceCommand);
                AddOption("DisplayName", _serviceName);
                AddOption("obj", string.IsNullOrWhiteSpace(_username) ? LocalServiceAccountName : _username);
                if (string.IsNullOrWhiteSpace(_username) == false)
                    AddOption("password", _password ?? string.Empty);

                var result = RunServiceControlTool(createArgs);

                switch ((uint)result.ExitCode)
                {
                    case 0:
                        var descriptionResult = RunServiceControlTool(["description", normalizedServiceName, WindowServiceDescription]);
                        if (descriptionResult.ExitCode != 0)
                            Console.WriteLine($"Service {ServiceFullName} was registered, but its description could not be set: { FormatServiceControlToolError(descriptionResult) }");

                        Console.WriteLine($"Service {ServiceFullName} has been registered.");
                        break;

                    case ErrorServiceExists:
                        Console.WriteLine($"Service {ServiceFullName} already exists. Reinstalling...");
                        Reinstall(serviceController, ravenServerDir, serviceArgs);
                        break;

                    case ErrorServiceMarkedForDeletion:
                        if (counter < 10)
                        {
                            Console.WriteLine($"Service {ServiceFullName} has been marked for deletion. Performing {counter + 1} installation attempt.");

                            Thread.Sleep(1000);
                            counter++;

                            InstallInternal(serviceController, ravenServerDir, serviceArgs, counter);
                        }
                        break;

                    case ErrorAccessIsDenied:
                        Console.WriteLine($"Cannot register service {ServiceFullName} due to insufficient privileges. Please use Administrator account to install the service.");
                        break;

                    default:
                        Console.WriteLine($"Cannot register service {ServiceFullName}: { FormatServiceControlToolError(result) }");
                        break;
                }
            }

            private static string FormatWin32ErrorMessage(Win32Exception exception)
            {
                return $"{exception.Message} (ERROR CODE 0x{exception.NativeErrorCode:x8}).";
            }

            private static string FormatServiceControlToolError((int ExitCode, string Output) result)
            {
                var message = string.IsNullOrWhiteSpace(result.Output) ? "sc.exe reported a failure." : result.Output;
                return $"{message} (ERROR CODE 0x{result.ExitCode:x8}).";
            }

            // The .NET BCL exposes no managed API to create/delete a Windows service, so register
            // and unregister shell out to sc.exe (the tool ships in %SystemRoot%\System32). sc.exe
            // returns the underlying Win32 error code as its process exit code, which lets the
            // callers branch on the same ERROR_* codes used elsewhere in this file.
            private static (int ExitCode, string Output) RunServiceControlTool(IReadOnlyList<string> arguments)
            {
                var startInfo = new ProcessStartInfo
                {
                    FileName = ServiceControlToolPath,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                };

                foreach (var argument in arguments)
                    startInfo.ArgumentList.Add(argument);

                using (var process = Process.Start(startInfo))
                {
                    var output = process.StandardOutput.ReadToEnd();
                    var error = process.StandardError.ReadToEnd();
                    process.WaitForExit();

                    var combined = string.Join(
                        Environment.NewLine,
                        new[] { output, error }.Where(part => string.IsNullOrWhiteSpace(part) == false));

                    return (process.ExitCode, combined.Trim());
                }
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

                var result = RunServiceControlTool(["delete", NormalizeServiceName(_serviceName)]);

                switch ((uint)result.ExitCode)
                {
                    case 0:
                        Console.WriteLine($"Service {ServiceFullName} has been unregistered.");
                        break;

                    case ErrorAccessIsDenied:
                        Console.WriteLine($"Cannot unregister service {ServiceFullName} due to insufficient privileges. Please use Administrator account to uninstall the service.");
                        break;

                    default:
                        Console.WriteLine($"Cannot unregister service {ServiceFullName}: { FormatServiceControlToolError(result) }");
                        break;
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

                    var serverExecutable = Path.Combine(serverDirInfo.FullName, "Raven.Server.exe");
                    if (File.Exists(serverExecutable) == false)
                    {
                        throw new ArgumentException($"Could not find RavenDB Server executable under {serverDirInfo.FullName}.");
                    }

                    if (argsForService.Any(x => x.StartsWith("--service-name")) == false)
                    {
                        argsForService.Add("--service-name");
                        argsForService.Add(_serviceName);
                    }

                    // Build the SCM ImagePath (executable + arguments) with the shared escaper so
                    // the executable path and each argument are quoted correctly, e.g. when the
                    // installation directory or an argument value contains spaces.
                    return CommandLineArgumentEscaper.EscapeAndConcatenate(argsForService.Prepend(serverExecutable));
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
